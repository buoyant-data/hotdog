/**
 * The settings module contains the necessary structs and code to process the
 * hotdog.yml file format
 */
use serde_json::Value;
use tracing::log::*;
use uuid::Uuid;

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

pub fn load(file: &str) -> Settings {
    let conf = load_configuration(file);
    let mut settings: Settings = conf
        .try_into()
        .expect("Failed to parse the configuration file");
    settings.populate_caches();
    settings
}

fn load_configuration(file: &str) -> config::Config {
    let file_path = Path::new(file);

    if file_path.extension().unwrap() != "yml" {
        panic!("The configuration file must end with .yml");
    }

    debug!("Loading configuration from {}", file);

    /*
     * Load our settings in the priority order of:
     *
     *   - yaml file
     *   - environment variables
     *
     * Each layer overriding properties from the last
     */
    let mut conf = config::Config::default();
    conf.merge(config::File::with_name(file))
        .unwrap()
        .merge(config::Environment::with_prefix("HOTDOG"))
        .unwrap();

    let _port: u64 = conf
        .get("global.listen.port")
        .expect("Configuration had no `global.listen.port` setting");
    conf
}

/**
 * Valid field to apply the rule upon
 *
 * They should be camel-cased in the yaml configuration
 */
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Field {
    Severity,
    Facility,
    Hostname,
    Appname,
    Msg,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum Action {
    Forward {
        topic: String,
    },
    Merge {
        json: Value,
        #[serde(default = "default_none")]
        json_str: Option<String>,
    },
    Replace {
        template: String,
    },
    Stop,
}

impl Action {
    fn populate_caches(&mut self) {
        if let Action::Merge { json, json_str } = self {
            *json_str =
                Some(serde_json::to_string(json).expect("Failed to serialize Merge action"));
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Rule {
    #[serde(skip_serializing, skip_deserializing, default = "default_uuid")]
    pub uuid: Uuid,
    pub field: Field,
    pub actions: Vec<Action>,
    #[serde(with = "serde_regex", default = "default_none")]
    pub regex: Option<regex::Regex>,
    #[serde(default = "default_none")]
    pub jmespath: Option<String>,
}

impl Rule {
    fn populate_caches(&mut self) {
        self.actions.iter_mut().for_each(|action| {
            action.populate_caches();
        });
    }
}
impl std::fmt::Display for Rule {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        if let Some(regex) = &self.regex {
            write!(f, "Regex: {}", regex)
        } else {
            write!(f, "JMESPath: {}", self.jmespath.as_ref().unwrap())
        }
    }
}

#[derive(Debug, Default, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum TlsType {
    #[default]
    None,

    /**
     * The CertsAndKey struct will contain PathBuf values if the tls section was
     * included
     */
    CertAndKey {
        cert: std::path::PathBuf,
        key: std::path::PathBuf,
        ca: Option<std::path::PathBuf>,
    },
}

#[derive(Debug, Deserialize)]
pub struct Listen {
    pub address: String,
    pub port: u64,
    #[serde(default)]
    pub tls: TlsType,
}

#[derive(Debug, Deserialize)]
pub struct Kafka {
    #[serde(default = "kafka_buffer_default")]
    pub buffer: usize,
    #[serde(default = "kafka_timeout_default")]
    pub timeout_ms: Duration,
    pub conf: HashMap<String, String>,
    pub topic: String,
}

#[derive(Debug, Deserialize)]
pub struct Metrics {
    pub statsd: String,
}

#[derive(Debug, Deserialize)]
pub struct Status {
    pub address: String,
    pub port: u64,
}

#[derive(Debug, Deserialize)]
pub struct Global {
    pub kafka: Kafka,
    pub listen: Listen,
    pub metrics: Metrics,
    pub status: Option<Status>,
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub global: Global,
    pub rules: Vec<Rule>,
}

impl Settings {
    /**
     * Populate any configuration caches which we want to us
     */
    fn populate_caches(&mut self) {
        self.rules.iter_mut().for_each(|rule| {
            rule.populate_caches();
        });
    }
}

/// Return the default size used for the Kafka buffer
fn kafka_buffer_default() -> usize {
    1024
}

fn kafka_timeout_default() -> Duration {
    Duration::from_secs(30)
}

fn default_none<T>() -> Option<T> {
    None
}

fn default_uuid() -> Uuid {
    Uuid::new_v4()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_example_config() {
        load("hotdog.yml");
    }

    #[test]
    fn test_load_example_and_populate_caches() {
        let settings = load("test/configs/single-rule-with-merge.yml");
        assert_eq!(settings.rules.len(), 1);
        match &settings.rules[0].actions[0] {
            Action::Merge { json: _, json_str } => {
                assert!(json_str.is_some());
            }
            _ => {
                assert!(false);
            }
        }
    }

    #[test]
    fn test_default_tls() {
        assert_eq!(TlsType::None, TlsType::default());
    }

    #[test]
    fn test_kafka_buffer_default() {
        assert_eq!(1024, kafka_buffer_default());
    }

    #[test]
    fn test_default_uuid() {
        assert_eq!(false, default_uuid().is_nil());
    }
}
