use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::{Error, Result};

/// Immutable inputs used to create one JavaScript application generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationDefinition {
    frameworks: Option<Vec<String>>,
    icon: Option<String>,
    name: Arc<str>,
    main_module: PathBuf,
    instances: usize,
    environment: Arc<BTreeMap<String, String>>,
    env_file: Vec<PathBuf>,
}

impl ApplicationDefinition {
    pub fn new(
        name: impl Into<Arc<str>>,
        main_module: impl Into<PathBuf>,
    ) -> Result<Self> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(Error::EmptyApplicationName);
        }

        let main_module = main_module.into();
        if !main_module.is_absolute() {
            return Err(Error::InvalidModulePath(main_module));
        }

        Ok(Self {
            frameworks: None,
            icon: None,
            name,
            main_module,
            instances: 1,
            environment: Arc::default(),
            env_file: Vec::new(),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Declare management metadata, overriding package.json detection.
    /// This does not change module loading or request dispatch.
    pub fn framework(
        mut self,
        framework: impl Into<String>,
    ) -> Self {
        self.frameworks = Some(vec![framework.into()]);
        self
    }

    pub fn frameworks(
        mut self,
        frameworks: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.frameworks =
            Some(frameworks.into_iter().map(Into::into).collect());
        self
    }

    pub fn framework_names(&self) -> Option<&[String]> {
        self.frameworks.as_deref()
    }

    /// Symbolic admin-client icon code; does not load a file or change execution.
    pub fn icon(
        mut self,
        icon: impl Into<String>,
    ) -> Self {
        self.icon = Some(icon.into());
        self
    }

    pub fn icon_name(&self) -> Option<&str> {
        self.icon.as_deref()
    }

    pub fn main_module(&self) -> &Path {
        &self.main_module
    }

    pub fn instances(
        mut self,
        instances: usize,
    ) -> Result<Self> {
        if instances == 0 {
            return Err(Error::InvalidInstanceCount);
        }
        self.instances = instances;
        Ok(self)
    }

    pub fn instance_count(&self) -> usize {
        self.instances
    }

    pub fn environment(
        mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        Arc::make_mut(&mut self.environment).insert(name.into(), value.into());
        self
    }

    pub fn environments(
        mut self,
        values: impl IntoIterator<Item = (String, String)>,
    ) -> Self {
        Arc::make_mut(&mut self.environment).extend(values);
        self
    }

    pub fn environment_values(&self) -> Arc<BTreeMap<String, String>> {
        self.environment.clone()
    }

    /// Raw execution sources, resolved with the selected deployment variant.
    pub fn env_files(
        mut self,
        files: impl IntoIterator<Item = PathBuf>,
    ) -> Self {
        self.env_file = files.into_iter().collect();
        self
    }

    pub fn environment_files(&self) -> &[PathBuf] {
        &self.env_file
    }

    pub(crate) fn resolved_environment(
        mut self,
        values: BTreeMap<String, String>,
    ) -> Self {
        self.environment = Arc::new(values);
        self
    }
}

#[cfg(test)]
#[path = "../../tests/application/definition.rs"]
mod tests;
