//! `claviron:orbit/cell` — a typed fleet cell as a JavaScript value.
//!
//! The JavaScript handle holds only the id. Every op opens the cell by id,
//! which is a tag-and-generation check on shared memory, so a released cell
//! fails as stale in every isolate that still holds it. Values cross the
//! boundary as strings: an int cell is a BigInt in JavaScript, and a string
//! carries it without losing the high bits.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use claviron::orbit::cell::{CellId, Claim, Orbital, TextId};
use claviron::orbit::{Cells, Text};
use deno_core::{OpState, op2};
use deno_error::JsErrorBox;
use serde::{Deserialize, Serialize};

use super::{OrbitContext, js_error};

#[derive(Debug, Deserialize)]
pub(super) struct AllocateRequest {
    kind: String,
    value: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct IdRequest {
    id: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct ValueRequest {
    id: String,
    value: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct AddRequest {
    id: String,
    by: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct AddUntilRequest {
    id: String,
    by: String,
    limit: String,
}

/// What one bounded claim did. `took` false is the whole answer: nothing was
/// added because nothing was left.
#[derive(Debug, Serialize)]
pub(super) struct ClaimReply {
    took: bool,
    taken: String,
    after: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct AppendRequest {
    id: String,
    text: String,
}

fn failure(error: impl std::fmt::Display) -> JsErrorBox {
    JsErrorBox::generic(format!("Orbital: {error}"))
}

fn parse<T: std::str::FromStr>(
    value: &str,
    message: &str,
) -> Result<T, JsErrorBox> {
    value.parse().map_err(|_| failure(message))
}

fn cells(state: &OpState) -> Result<Arc<Cells>, JsErrorBox> {
    state.borrow::<OrbitContext>().cells().map_err(js_error)
}

enum Cell {
    Int(Orbital<i64>),
    Float(Orbital<f64>),
    Bool(Orbital<bool>),
    Text(Text),
}

impl Cell {
    fn allocate(
        cells: &Cells,
        kind: &str,
        value: &str,
    ) -> Result<Self, JsErrorBox> {
        Ok(match kind {
            "int" => Self::Int(
                cells
                    .allocate(parse::<i64>(
                        value,
                        "an int cell takes an integer",
                    )?)
                    .map_err(failure)?,
            ),
            "float" => Self::Float(
                cells
                    .allocate(parse::<f64>(
                        value,
                        "a float cell takes a number",
                    )?)
                    .map_err(failure)?,
            ),
            "bool" => Self::Bool(
                cells
                    .allocate(parse::<bool>(
                        value,
                        "a bool cell takes a boolean",
                    )?)
                    .map_err(failure)?,
            ),
            "string" => {
                Self::Text(cells.allocate_text(value).map_err(failure)?)
            }
            other => return Err(failure(format!("unknown cell kind {other}"))),
        })
    }

    fn open(
        cells: &Cells,
        id: &str,
    ) -> Result<Self, JsErrorBox> {
        if let Some(rest) = id.strip_prefix("cell:") {
            // The scalar id does not say which type; the cell does.
            let cell_id: CellId =
                format!("cell:{rest}").parse().map_err(failure)?;
            return cells
                .open::<i64>(cell_id)
                .map(Self::Int)
                .or_else(|_| cells.open::<f64>(cell_id).map(Self::Float))
                .or_else(|_| cells.open::<bool>(cell_id).map(Self::Bool))
                .map_err(failure);
        }
        let text_id: TextId = id.parse().map_err(failure)?;
        cells.open_text(text_id).map(Self::Text).map_err(failure)
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::Int(_) => "int",
            Self::Float(_) => "float",
            Self::Bool(_) => "bool",
            Self::Text(_) => "string",
        }
    }

    fn id(&self) -> String {
        match self {
            Self::Int(cell) => cell.id().to_string(),
            Self::Float(cell) => cell.id().to_string(),
            Self::Bool(cell) => cell.id().to_string(),
            Self::Text(cell) => cell.id().to_string(),
        }
    }

    fn load(&self) -> Result<String, JsErrorBox> {
        Ok(match self {
            Self::Int(cell) => cell.load().map_err(failure)?.to_string(),
            Self::Float(cell) => cell.load().map_err(failure)?.to_string(),
            Self::Bool(cell) => cell.load().map_err(failure)?.to_string(),
            Self::Text(cell) => cell.load().map_err(failure)?,
        })
    }

    fn store(
        &self,
        value: &str,
    ) -> Result<(), JsErrorBox> {
        match self {
            Self::Int(cell) => {
                cell.store(parse(value, "an int cell takes an integer")?)
            }
            Self::Float(cell) => {
                cell.store(parse(value, "a float cell takes a number")?)
            }
            Self::Bool(cell) => {
                cell.store(parse(value, "a bool cell takes a boolean")?)
            }
            Self::Text(cell) => cell.store(value),
        }
        .map_err(failure)
    }

    /// Atomic on the cell; the value after comes back.
    fn add(
        &self,
        by: &str,
    ) -> Result<String, JsErrorBox> {
        match self {
            Self::Int(cell) => cell
                .fetch_add(parse(by, "an int cell takes an integer")?)
                .map(|value| value.to_string()),
            Self::Float(cell) => cell
                .fetch_add(parse(by, "a float cell takes a number")?)
                .map(|value| value.to_string()),
            Self::Bool(_) => {
                return Err(failure("a bool cell has no arithmetic"));
            }
            Self::Text(_) => {
                return Err(failure(
                    "a string cell has no arithmetic; use append()",
                ));
            }
        }
        .map_err(failure)
    }

    /// Take up to `by` while the value stays at or under `limit`, in one
    /// swap. The point is that N callers racing this never pass the limit,
    /// which `add` followed by a test cannot promise.
    fn add_until(
        &self,
        by: &str,
        limit: &str,
    ) -> Result<ClaimReply, JsErrorBox> {
        let Self::Int(cell) = self else {
            return Err(failure("only an int cell can be claimed from"));
        };
        let by = parse(by, "a claim takes an integer")?;
        let limit = parse(limit, "a limit takes an integer")?;
        match cell.add_until(by, limit).map_err(failure)? {
            Claim::Took { taken, after } => Ok(ClaimReply {
                took: true,
                taken: taken.to_string(),
                after: after.to_string(),
            }),
            Claim::Exhausted => Ok(ClaimReply {
                took: false,
                taken: "0".into(),
                after: limit.to_string(),
            }),
        }
    }

    fn append(
        &self,
        text: &str,
    ) -> Result<String, JsErrorBox> {
        let Self::Text(cell) = self else {
            return Err(failure("only a string cell can be appended to"));
        };
        cell.append(text).map_err(failure)?;
        cell.load().map_err(failure)
    }

    fn version(&self) -> Result<u32, JsErrorBox> {
        match self {
            Self::Int(cell) => cell.version(),
            Self::Float(cell) => cell.version(),
            Self::Bool(cell) => cell.version(),
            Self::Text(cell) => cell.version(),
        }
        .map_err(failure)
    }

    /// Parks until the cell is written or `interrupt` is set and the cell
    /// is woken. Blocks; called off the isolate thread. `Ok(None)` is the
    /// interrupt, never a clock.
    fn wait_changed_until(
        &self,
        since: u32,
        interrupt: &AtomicBool,
    ) -> Result<Option<u32>, JsErrorBox> {
        match self {
            Self::Int(cell) => cell.wait_changed_until(since, interrupt),
            Self::Float(cell) => cell.wait_changed_until(since, interrupt),
            Self::Bool(cell) => cell.wait_changed_until(since, interrupt),
            Self::Text(cell) => cell.wait_changed_until(since, interrupt),
        }
        .map_err(failure)
    }

    /// Wake whoever is parked on this cell without writing to it.
    fn wake_waiters(&self) -> Result<(), JsErrorBox> {
        match self {
            Self::Int(cell) => cell.wake_waiters(),
            Self::Float(cell) => cell.wake_waiters(),
            Self::Bool(cell) => cell.wake_waiters(),
            Self::Text(cell) => cell.wake_waiters(),
        }
        .map_err(failure)
    }

    fn release(self) -> Result<(), JsErrorBox> {
        match self {
            Self::Int(cell) => cell.release(),
            Self::Float(cell) => cell.release(),
            Self::Bool(cell) => cell.release(),
            Self::Text(cell) => cell.release(),
        }
        .map_err(failure)
    }
}

#[op2]
#[string]
pub(super) fn op_claviron_orbit_cell_allocate(
    state: &OpState,
    #[serde] request: AllocateRequest,
) -> Result<String, JsErrorBox> {
    let cells = cells(state)?;
    Cell::allocate(&cells, &request.kind, &request.value).map(|cell| cell.id())
}

#[op2]
#[string]
pub(super) fn op_claviron_orbit_cell_open(
    state: &OpState,
    #[serde] request: IdRequest,
) -> Result<String, JsErrorBox> {
    let cells = cells(state)?;
    Cell::open(&cells, &request.id).map(|cell| cell.kind().to_owned())
}

#[op2]
#[string]
pub(super) fn op_claviron_orbit_cell_get(
    state: &OpState,
    #[serde] request: IdRequest,
) -> Result<String, JsErrorBox> {
    let cells = cells(state)?;
    Cell::open(&cells, &request.id)?.load()
}

#[op2]
pub(super) fn op_claviron_orbit_cell_set(
    state: &OpState,
    #[serde] request: ValueRequest,
) -> Result<(), JsErrorBox> {
    let cells = cells(state)?;
    Cell::open(&cells, &request.id)?.store(&request.value)
}

#[op2]
#[string]
pub(super) fn op_claviron_orbit_cell_add(
    state: &OpState,
    #[serde] request: AddRequest,
) -> Result<String, JsErrorBox> {
    let cells = cells(state)?;
    Cell::open(&cells, &request.id)?.add(&request.by)
}

#[op2]
#[serde]
pub(super) fn op_claviron_orbit_cell_add_until(
    state: &OpState,
    #[serde] request: AddUntilRequest,
) -> Result<ClaimReply, JsErrorBox> {
    let cells = cells(state)?;
    Cell::open(&cells, &request.id)?.add_until(&request.by, &request.limit)
}

#[op2]
#[string]
pub(super) fn op_claviron_orbit_cell_append(
    state: &OpState,
    #[serde] request: AppendRequest,
) -> Result<String, JsErrorBox> {
    let cells = cells(state)?;
    Cell::open(&cells, &request.id)?.append(&request.text)
}

#[derive(Debug, Deserialize)]
pub(super) struct WaitRequest {
    id: String,
    since: u32,
}

#[op2(fast)]
pub(super) fn op_claviron_orbit_cell_version(
    state: &OpState,
    #[string] id: String,
) -> Result<u32, JsErrorBox> {
    let cells = cells(state)?;
    Cell::open(&cells, &id)?.version()
}

/// Wake whoever is parked on this cell without writing to it. On its own
/// this changes nothing — the waiter re-checks the count, finds it where it
/// was and parks again — so it is only meaningful after the word the waiter
/// also watches has been set.
pub(super) fn wake_waiters(
    cells: &Cells,
    id: &str,
) {
    if let Ok(cell) = Cell::open(cells, id) {
        let _ = cell.wake_waiters();
    }
}

/// Parks a blocking-pool thread on the cell's change count, so the isolate
/// keeps running while one script awaits a write from another process. A
/// released cell ends the wait with an error, and so does this isolate
/// going away: a script cannot be left parked on a write that will never
/// come because the process is shutting down.
///
/// The wait is registered before it parks, so shutdown knows which cells
/// to wake. Nothing here measures time — the thread parks until the
/// kernel releases it, and shutdown releases it by setting the word the
/// waiter also watches and then waking the cell.
#[op2]
pub(super) async fn op_claviron_orbit_cell_wait_changed(
    state: Rc<RefCell<OpState>>,
    #[serde] request: WaitRequest,
) -> Result<u32, JsErrorBox> {
    let (cells, waits) = {
        let state = state.borrow();
        (cells(&state)?.clone(), state.borrow::<OrbitContext>().cell_waits())
    };

    let parked = waits.register(cells.clone(), request.id.clone());
    let since = request.since;
    let id = request.id.clone();
    let waiting = Arc::clone(&waits);
    let outcome = tokio::task::spawn_blocking(move || {
        Cell::open(&cells, &id)?
            .wait_changed_until(since, waiting.interrupted())
    })
    .await;
    waits.finish(parked);

    match outcome
        .map_err(|error| failure(format!("wait interrupted: {error}")))??
    {
        Some(changes) => Ok(changes),
        None => {
            Err(failure("cell wait ended: this JavaScript runtime is stopping"))
        }
    }
}

#[op2]
pub(super) fn op_claviron_orbit_cell_release(
    state: &OpState,
    #[serde] request: IdRequest,
) -> Result<(), JsErrorBox> {
    let cells = cells(state)?;
    Cell::open(&cells, &request.id)?.release()
}
