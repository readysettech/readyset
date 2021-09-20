use std::collections::HashMap;
use std::fmt::{self, Write};

use derive_more::{From, Into};
use serde::de::{self, Unexpected, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_with::skip_serializing_none;
use serde_yaml::Value;
use test_strategy::Arbitrary;

fn is_default<T>(x: &T) -> bool
where
    T: Default + PartialEq,
{
    x == &T::default()
}

fn default_required() -> bool {
    true
}

#[derive(Debug, Eq, PartialEq, Clone, From, Into, Arbitrary)]
pub struct StringOrList(Vec<String>);

impl From<String> for StringOrList {
    fn from(s: String) -> Self {
        Self(vec![s])
    }
}

impl Serialize for StringOrList {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if self.0.len() == 1 {
            self.0.first().unwrap().serialize(serializer)
        } else {
            self.0.serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for StringOrList {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct StringOrListVisitor;
        impl<'de> Visitor<'de> for StringOrListVisitor {
            type Value = StringOrList;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a command or a list of commands")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(StringOrList(vec![v.to_owned()]))
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(StringOrList(vec![v]))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: de::SeqAccess<'de>,
            {
                let mut vs = match seq.size_hint() {
                    Some(size) => Vec::with_capacity(size),
                    None => Vec::new(),
                };

                while let Some(v) = seq.next_element()? {
                    vs.push(v);
                }

                Ok(StringOrList(vs))
            }
        }

        deserializer.deserialize_any(StringOrListVisitor)
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Plugin {
    pub name: String,
    pub params: Value,
}

impl Serialize for Plugin {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry(&self.name, &self.params)?;
        map.end()
    }
}

impl<'de> Deserialize<'de> for Plugin {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct PluginVisitor;
        impl<'de> Visitor<'de> for PluginVisitor {
            type Value = Plugin;
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a buildkite plugin definition")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: de::MapAccess<'de>,
            {
                let (name, params) = map.next_entry()?.ok_or_else(|| {
                    de::Error::custom(
                        "plugin maps must contain exactly one key:value pair; found 0",
                    )
                })?;
                let result = Plugin { name, params };
                if map.size_hint().into_iter().any(|x| x > 0) {
                    return Err(de::Error::custom(
                        "plugin maps must contain exactly one key:value pair; found more than one",
                    ));
                }
                Ok(result)
            }
        }

        deserializer.deserialize_map(PluginVisitor)
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Default, From, Into, Serialize)]
pub struct Plugins(Vec<Plugin>);

impl<'de> Deserialize<'de> for Plugins {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct PluginsVisitor;
        impl<'de> Visitor<'de> for PluginsVisitor {
            type Value = Plugins;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("an array or map of plugins")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: de::MapAccess<'de>,
            {
                let mut plugins = match map.size_hint() {
                    Some(size) => Vec::with_capacity(size),
                    None => Vec::new(),
                };

                while let Some((name, params)) = map.next_entry()? {
                    plugins.push(Plugin { name, params })
                }

                Ok(Plugins(plugins))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: de::SeqAccess<'de>,
            {
                let mut plugins = match seq.size_hint() {
                    Some(size) => Vec::with_capacity(size),
                    None => Vec::new(),
                };

                while let Some(plugin) = seq.next_element()? {
                    plugins.push(plugin);
                }

                Ok(Plugins(plugins))
            }
        }

        deserializer.deserialize_any(PluginsVisitor)
    }
}

#[skip_serializing_none]
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct CommandStep {
    #[serde(alias = "commands")]
    pub command: Option<StringOrList>,

    pub label: Option<String>,

    pub key: Option<String>,

    pub agents: Option<HashMap<String, String>>,

    #[serde(default, skip_serializing_if = "is_default")]
    pub allow_dependency_failure: bool,

    pub artifact_paths: Option<StringOrList>,

    pub branches: Option<String>,

    #[serde(default, skip_serializing_if = "is_default")]
    pub plugins: Plugins,

    pub depends_on: Option<Vec<String>>,

    #[serde(default, skip_serializing_if = "is_default")]
    pub env: HashMap<String, Value>,

    pub timeout_in_minutes: Option<usize>,
}

#[derive(Debug, Eq, PartialEq, Default, Clone, Arbitrary)]
pub struct WaitStep {
    pub continue_on_failure: bool,

    pub condition: Option<String>,

    pub depends_on: Option<Vec<String>>,

    pub allow_dependency_failure: bool,
}

impl Serialize for WaitStep {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut len = 1;
        if self.continue_on_failure {
            len += 1;
        }
        if self.condition.is_some() {
            len += 1;
        }
        if self.depends_on.is_some() {
            len += 1;
        }
        if self.allow_dependency_failure {
            len += 1;
        }

        let mut map = serializer.serialize_map(Some(len))?;
        map.serialize_entry("wait", &Value::Null)?;
        if self.continue_on_failure {
            map.serialize_entry("continue_on_failure", &true)?;
        }
        if let Some(condition) = &self.condition {
            map.serialize_entry("if", condition)?;
        }
        if let Some(depends_on) = &self.depends_on {
            map.serialize_entry("depends_on", depends_on)?;
        }
        if self.allow_dependency_failure {
            map.serialize_entry("allow_dependency_failure", &true)?;
        }

        map.end()
    }
}

impl<'de> Deserialize<'de> for WaitStep {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct WaitStepVisitor;
        impl<'de> Visitor<'de> for WaitStepVisitor {
            type Value = WaitStep;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("the string \"wait\"")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if v != "wait" {
                    return Err(de::Error::invalid_value(Unexpected::Str(v), &self));
                }

                Ok(WaitStep::default())
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: de::MapAccess<'de>,
            {
                let mut res = WaitStep::default();
                let mut seen_wait = false;
                while let Some(k) = map.next_key::<String>()? {
                    match k.as_str() {
                        "wait" => seen_wait = true,
                        "continue_on_failure" => res.continue_on_failure = map.next_value()?,
                        "if" => res.condition = Some(map.next_value()?),
                        "depends_on" => res.depends_on = Some(map.next_value()?),
                        "allow_dependency_failure" => {
                            res.allow_dependency_failure = map.next_value()?
                        }
                        _ => {}
                    }
                }

                if !seen_wait {
                    return Err(de::Error::missing_field("wait"));
                }

                Ok(res)
            }
        }

        deserializer.deserialize_any(WaitStepVisitor)
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct TextField {
    #[serde(rename = "text")]
    pub label: String,

    pub key: String,

    pub hint: Option<String>,

    #[serde(default = "default_required", skip_serializing_if = "Clone::clone")]
    pub required: bool,

    pub default: Option<String>,
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct SelectOption {
    pub label: String,
    pub value: String,
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SelectFieldDefault {
    Single(String),
    Multiple(Vec<String>),
}

#[skip_serializing_none]
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct SelectField {
    #[serde(rename = "select")]
    pub label: String,

    pub key: String,

    pub options: Vec<SelectOption>,

    pub hint: Option<String>,

    #[serde(default = "default_required", skip_serializing_if = "Clone::clone")]
    pub required: bool,

    #[serde(default, skip_serializing_if = "is_default")]
    pub multiple: bool,

    pub default: Option<SelectFieldDefault>,
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Field {
    Text(TextField),
    Select(SelectField),
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BlockedState {
    Passed,
    Failed,
    Running,
}

impl Default for BlockedState {
    fn default() -> Self {
        Self::Passed
    }
}

#[skip_serializing_none]
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct BlockStep {
    #[serde(rename = "block")]
    pub label: String,

    pub key: Option<String>,

    pub prompt: Option<String>,

    #[serde(default, skip_serializing_if = "is_default")]
    pub fields: Option<Vec<Field>>,

    #[serde(default, skip_serializing_if = "is_default")]
    pub blocked_state: BlockedState,

    pub branches: Option<String>,

    #[serde(rename = "if")]
    pub condition: Option<String>,

    pub depends_on: Option<Vec<String>>,

    #[serde(default, skip_serializing_if = "is_default")]
    pub allow_dependency_failure: bool,
}

#[skip_serializing_none]
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct InputStep {
    #[serde(rename = "input")]
    pub label: String,

    pub key: Option<String>,

    pub prompt: Option<String>,

    #[serde(default, skip_serializing_if = "is_default")]
    pub fields: Option<Vec<Field>>,

    #[serde(default, skip_serializing_if = "is_default")]
    pub blocked_state: BlockedState,

    pub branches: Option<String>,

    #[serde(rename = "if")]
    pub condition: Option<String>,

    pub depends_on: Option<Vec<String>>,

    #[serde(default, skip_serializing_if = "is_default")]
    pub allow_dependency_failure: bool,
}

#[skip_serializing_none]
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct BuildAttributes {
    pub branch: Option<String>,
    pub commit: Option<String>,
    pub env: Option<HashMap<String, String>>,
    pub message: Option<String>,
    pub meta_data: Option<HashMap<String, String>>,
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SkipStep {
    Skip(bool),
    SkipReason(String),
}

impl Default for SkipStep {
    fn default() -> Self {
        Self::Skip(false)
    }
}

#[skip_serializing_none]
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct TriggerStep {
    #[serde(rename = "trigger")]
    pub pipeline: String,

    pub build: Option<BuildAttributes>,

    #[serde(rename = "async", default, skip_serializing_if = "is_default")]
    pub async_build: bool,

    pub branches: Option<String>,

    #[serde(rename = "if")]
    pub condition: Option<String>,

    pub depends_on: Option<Vec<String>>,

    #[serde(default, skip_serializing_if = "is_default")]
    pub allow_dependency_failure: bool,

    #[serde(default, skip_serializing_if = "is_default")]
    pub skip: SkipStep,
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize)]
#[serde(untagged)]
pub enum Step {
    Command(CommandStep),
    Wait(WaitStep),
    Block(BlockStep),
    Input(InputStep),
    Trigger(TriggerStep),
}

impl<'de> Deserialize<'de> for Step {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let mut errs = vec![];
        CommandStep::deserialize(value.clone())
            .map(Step::Command)
            .or_else(|e| {
                errs.push(e);
                WaitStep::deserialize(value.clone()).map(Step::Wait)
            })
            .map_err(|e| {
                errs.push(e);
                let mut msg = format!("could not deserialize step due to {} errors:", errs.len());
                for err in errs {
                    write!(msg, "\n - {}", err).unwrap();
                }
                de::Error::custom(msg)
            })
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct Pipeline {
    pub steps: Vec<Step>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_yaml as yaml;
    use test_strategy::proptest;

    #[test]
    fn ecr_plugin() {
        let src = "
ecr#v2.2.0:
  login: true";
        let res = yaml::from_str::<Plugin>(src);
        assert!(res.is_ok(), "{}", res.err().unwrap());
        assert_eq!(res.unwrap().name, "ecr#v2.2.0");
    }

    macro_rules! serde_round_trips {
        () => {};
        ($test_name:ident($type: ty); $($rest:tt)*) => {
            #[proptest]
            fn $test_name(val: $type) {
                let serialized = yaml::to_string(&val).unwrap();
                let roundtripped: $type = yaml::from_str(&serialized).unwrap();
                assert_eq!(roundtripped, val);
            }

            serde_round_trips!($($rest)*);
        };
    }

    serde_round_trips! {
        string_or_list_round_trip(StringOrList);
        wait_step_round_trip(WaitStep);
    }

    macro_rules! parses {
        () => {};
        ($test_name:ident($type: ty, $src: expr); $($rest:tt)*) => {
            #[test]
            fn $test_name() {
                let res =yaml::from_str::<$type>($src);
                assert!(res.is_ok(), "{}", res.err().unwrap());
            }

            parses!($($rest)*);
        }
    }

    parses! {
        build_build_image_command_step(CommandStep, "
label: ':docker: Build build image'
key: build-image
command: .buildkite/build_image.sh build/Dockerfile readyset-build
plugins:
  ecr#v2.2.0:
    login: true");

    run_logictests_command_step(CommandStep, "
label: 'Run MySQL generated logictests'
key: logictest-generated-mysql
command:
- 'echo +++ Running noria-logictest'
- cargo run --bin noria-logictest --release -- verify logictests/generated/mysql -t 1 --database-type mysql
depends_on:
- build-image
plugins:
    - docker-compose#v3.7.0:
        run: app
        env:
        - SCCACHE_BUCKET=readyset-sccache-e1
        - AWS_IAM_CREDENTIALS_URL=http://169.254.169.254/latest/meta-data/iam/security-credentials/buildkite-Role
        - CARGO_INCREMENTAL=0
        config:
        - docker-compose.yml
        - build/docker-compose.ci-test.yaml
    - ecr#v2.2.0:
        login: true
env:
    SCCACHE_BUCKET: readyset-sccache
agents:
    queue: nightly");

        run_fuzz_tests_command_step(CommandStep, "
label: 'Run fuzz tests'
key: logictest-fuzz
command:
- 'echo +++ Running noria-logictest'
- cargo run --bin noria-logictest -- fuzz -n 5000 --compare-to mysql://root:noria@mysql/noria
timeout_in_minutes: 360
depends_on:
- build-image
plugins:
    - docker-compose#v3.7.0:
        run: app
        env:
        - SCCACHE_BUCKET=readyset-sccache-e1
        - AWS_IAM_CREDENTIALS_URL=http://169.254.169.254/latest/meta-data/iam/security-credentials/buildkite-Role
        - CARGO_INCREMENTAL=0
        config:
        - docker-compose.yml
        - build/docker-compose.ci-test.yaml
    - ecr#v2.2.0:
        login: true
env:
    SCCACHE_BUCKET: readyset-sccache
    PROPTEST_MAX_SHRINK_ITERS: 1000
");

        wait_step_tilde(WaitStep, "wait: ~");
        wait_step_string(WaitStep, "wait");

        blocked_state_passed(BlockedState, "passed");
    }
}
