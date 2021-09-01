//! Due to the monorepo setup, we have many build steps set up in
//! `.buildkite/common.yaml` but we only want to execute some of them in any
//! particular commit. This verifies that all of the steps defined in
//! `.buildkite/common.yaml` have an `include` or `exclude` clause in the
//! `buildkite.yaml` which uses it.
use std::collections::HashSet;
use std::fs;

use anyhow::{bail, Result};

use crate::project_root_path;
use yaml_rust::{Yaml, YamlLoader};

use tracing::warn;

pub(crate) fn run() -> Result<()> {
    let project_path = project_root_path();
    let buildkite_yaml_path = project_path.join("buildkite.yaml");
    let buildkite_common_yaml_path = project_path.join(".buildkite").join("common.yaml");

    let buildkite_yaml = YamlLoader::load_from_str(&fs::read_to_string(buildkite_yaml_path)?)?;

    let buildkite_yaml_steps = buildkite_yaml
        .first()
        .unwrap()
        .as_hash()
        .unwrap()
        .get(&Yaml::from_str("steps"))
        .unwrap()
        .as_vec()
        .unwrap();

    let mut git_diff_conditional_config: Option<&Yaml> = None;
    for step in buildkite_yaml_steps {
        if git_diff_conditional_config.is_some() {
            break;
        }
        let step = step.as_hash().unwrap();
        let step_plugins = match step.get(&Yaml::from_str("plugins")) {
            Some(plugins) => plugins.as_vec().unwrap(),
            None => continue,
        };
        for step_plugin in step_plugins {
            if git_diff_conditional_config.is_some() {
                break;
            }
            let step_plugin = step_plugin.as_hash().unwrap();
            for (plugin_name, config) in step_plugin.iter() {
                let plugin_name = plugin_name.as_str().unwrap();
                if plugin_name.starts_with("Zegocover/git-diff-conditional") {
                    git_diff_conditional_config = Some(config);
                    break;
                }
            }
        }
    }
    let git_diff_conditional_config = match git_diff_conditional_config {
        Some(config) => config.as_hash().unwrap(),
        None => bail!("Could not get git diff conditional config"),
    };

    let mut git_diff_conditional_step_labels = HashSet::new();
    for step in git_diff_conditional_config
        .get(&Yaml::from_str("steps"))
        .unwrap()
        .as_vec()
        .unwrap()
    {
        let label = step["label"].as_str().unwrap();
        git_diff_conditional_step_labels.insert(label);
    }

    let buildkite_common_yaml =
        YamlLoader::load_from_str(&fs::read_to_string(buildkite_common_yaml_path)?)?;

    let buildkite_common_yaml_steps = buildkite_common_yaml
        .first()
        .unwrap()
        .as_hash()
        .unwrap()
        .get(&Yaml::from_str("steps"))
        .unwrap()
        .as_vec()
        .unwrap();

    for step in buildkite_common_yaml_steps {
        let step = step.as_hash().unwrap();
        let step_label = step
            .get(&Yaml::from_str("label"))
            .unwrap()
            .as_str()
            .unwrap();
        if !git_diff_conditional_step_labels.contains(step_label) {
            warn!("Missing step definition in buildkite.yaml for step in .buildkite/common.yaml labeled '{}'", step_label)
        }
    }
    Ok(())
}
