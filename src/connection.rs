//! The connection module is responsible for handling everything pertaining to a single inbound TCP
//! connection.
use async_channel::Sender;
use chrono::prelude::*;
use handlebars::Handlebars;
use smol::io::{AsyncBufReadExt, BufReader};
use smol::stream::StreamExt;
use tracing::log::*;

use std::collections::HashMap;
use std::sync::Arc;

use crate::errors;
use crate::merge;
use crate::parse;
use crate::rules;
use crate::settings::*;
use crate::sink::Message;
use crate::status::{Statistic, Stats};

/// RuleState exists to help carry state into merge/replacement functions and exists only during the
/// processing of rules
struct RuleState<'a> {
    variables: &'a HashMap<String, String>,
    hb: &'a handlebars::Handlebars<'a>,
    stats: Sender<Statistic>,
}

/// Simple type to capture a map of precompiled jmespath expressions
pub type JmesPathExpressions<'a> = HashMap<String, jmespath::Expression<'a>>;

pub struct Connection {
    /// A reference to the global Settings object for all configuration information
    settings: Arc<Settings>,
    /// The sender-side of the channel to our Kafka connection, allowing the logs read in to be
    /// sent over to the Kafka handler
    sender: Sender<Message>,
    stats: Sender<Statistic>,
}

impl Connection {
    pub fn new(settings: Arc<Settings>, sender: Sender<Message>, stats: Sender<Statistic>) -> Self {
        Connection {
            settings,
            sender,
            stats,
        }
    }

    /// connection_loop is responsible for handling incoming syslog streams connections
    pub async fn read_logs<R: smol::io::AsyncRead + std::marker::Unpin>(
        &self,
        reader: BufReader<R>,
    ) -> Result<(), errors::HotdogError> {
        let mut lines = reader.lines();

        let mut hb = Handlebars::new();
        let mut jmespaths = JmesPathExpressions::new();

        if !precompile_templates(&mut hb, self.settings.clone()) {
            error!(
                "Failing to precompile templates is a fatal error, not going to parse logs since the configuration is broken"
            );
            // TODO fix the Err types
            return Ok(());
        }

        if !precompile_jmespath(&mut jmespaths, self.settings.clone()) {
            error!(
                "Failing to precompile jmespaths is a fata error, not parsing this connection's logs because the configuration is broken"
            );
            // TODO fix the Err types
            return Ok(());
        }

        while let Some(line) = lines.next().await {
            let line = line?;
            debug!("log: {}", line);

            let parsed = parse::parse_line(line);

            if let Err(e) = &parsed {
                let _ = self.stats.send((Stats::LogParseError, 1)).await;
                error!("failed to parse message: {:?}", e);
                continue;
            }
            /*
             * Now that we've logged the error, let's unpack and bubble the error anyways
             *
             * Note: msg needs to be mutable so we can fish the `msg` out within it during a
             * simd_json parse
             */
            let mut msg = parsed.unwrap();
            let _ = self.stats.send((Stats::LineReceived, 1)).await;
            let mut continue_rules = true;
            debug!("parsed as: {}", msg.msg);

            for rule in self.settings.rules.iter() {
                /*
                 * If we have been told to stop processing rules, then it's time to bail on this log
                 * message
                 */
                if !continue_rules {
                    break;
                }

                // The output buffer that we will ultimately send along to the Kafka service
                let mut output = String::new();
                let mut rule_matches = false;
                let mut hash = HashMap::new();
                hash.insert("msg".to_string(), String::from(&msg.msg));
                hash.insert("version".to_string(), env!["CARGO_PKG_VERSION"].to_string());
                hash.insert("iso8601".to_string(), Utc::now().to_rfc3339());

                match rule.field {
                    Field::Msg => {
                        rule_matches = rules::apply_rule(rule, &msg.msg, &jmespaths, &mut hash);
                    }
                    Field::Appname => {
                        if let Some(appname) = &msg.appname {
                            rule_matches = rules::apply_rule(rule, appname, &jmespaths, &mut hash);
                        }
                    }
                    Field::Hostname => {
                        if let Some(hostname) = &msg.hostname {
                            rule_matches = rules::apply_rule(rule, hostname, &jmespaths, &mut hash);
                        }
                    }
                    Field::Severity => {
                        if let Some(severity) = &msg.severity {
                            rule_matches = rules::apply_rule(rule, severity, &jmespaths, &mut hash);
                        }
                    }
                    Field::Facility => {
                        if let Some(facility) = &msg.facility {
                            rule_matches = rules::apply_rule(rule, facility, &jmespaths, &mut hash);
                        }
                    }
                }

                /*
                 * This specific didn't match, so onto the next one
                 */
                if !rule_matches {
                    continue;
                }

                let rule_state = RuleState {
                    hb: &hb,
                    variables: &hash,
                    stats: self.stats.clone(),
                };

                /*
                 * Process the actions one the rule has matched
                 */
                for index in 0..rule.actions.len() {
                    let action = &rule.actions[index];
                    /*
                     * @stjepang says this will fix slow future polling
                     *
                     * The underlying problem here is that this _can_ be a very tight
                     * and CPU-bound loop under heavy load conditions. There is nothing
                     * inherent in smol (under async-std 1.6.x) which will properly
                     * yield to other tasks in the runtime.
                     */
                    smol::future::yield_now().await;

                    match action {
                        Action::Forward { topic } => {
                            /*
                             * If a custom output was never defined, just take the
                             * raw message and pass that along.
                             */
                            if output.is_empty() {
                                output = String::from(&msg.msg);
                            }

                            if let Ok(actual_topic) = hb.render_template(topic, &hash) {
                                debug!("Enqueueing for topic: `{}`", actual_topic);
                                /*
                                 * `output` is consumed by send_to_kafka, so the rest of the rules
                                 * should be skipped.
                                 */
                                let kmsg = Message::new(actual_topic, output);
                                let _ = self.sender.send(kmsg).await;
                                /*
                                 * Ensure that we're allowing other tasks to execute when we pass
                                 * things off to the channel
                                 *
                                 * See also https://github.com/stjepang/smol/issues/159
                                 */
                                smol::future::yield_now().await;
                                continue_rules = false;
                            } else {
                                error!("Failed to process the configured topic: `{}`", topic);
                                let _ = self.stats.send((Stats::TopicParseFailed, 1)).await;
                            }
                            break;
                        }

                        Action::Merge { json, json_str: _ } => {
                            debug!("merging JSON content: {}", json);
                            if let Ok(buffer) = perform_merge(
                                &mut msg.msg,
                                &template_id_for(rule, index),
                                &rule_state,
                            ) {
                                output = buffer;
                            } else {
                                continue_rules = false;
                            }
                        }

                        Action::Replace { template } => {
                            let template_id = template_id_for(rule, index);

                            debug!(
                                "replacing content with template: {} ({})",
                                template, template_id
                            );
                            if let Ok(rendered) = hb.render(&template_id, &hash) {
                                output = rendered;
                            }
                        }

                        Action::Stop => {
                            continue_rules = false;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

/// Generate a unique identifier for the given template
fn template_id_for(rule: &Rule, index: usize) -> String {
    format!("{}-{}", rule.uuid, index)
}

/// precompile_templates will register templates for all the Merge and Replace actions from the
/// settings
///
/// Will usually return a true, unless some setting parse failure occurred which is a critical
/// failure for the daemon
fn precompile_templates(hb: &mut Handlebars, settings: Arc<Settings>) -> bool {
    for rule in settings.rules.iter() {
        for index in 0..rule.actions.len() {
            match &rule.actions[index] {
                Action::Merge { json: _, json_str } => {
                    let template_id = template_id_for(rule, index);

                    if let Some(template) = json_str {
                        if let Err(e) = hb.register_template_string(&template_id, template) {
                            error!("Failed to register template! {}\n{}", e, template);
                            return false;
                        }
                    } else {
                        error!("Could not look up the json_str for a Merge action");
                        return false;
                    }
                }
                Action::Replace { template } => {
                    let template_id = format!("{}-{}", rule.uuid, index);
                    if let Err(e) = hb.register_template_string(&template_id, template) {
                        error!("Failed to register template! {}\n{}", e, template);
                        return false;
                    }
                }
                _ => {}
            }
        }
    }
    true
}

/// precompile_jmespath will pre-generate all the necessary JMESPath::Variable objects from the
/// configuration file and shove thoe in the map given to it
fn precompile_jmespath(map: &mut JmesPathExpressions, settings: Arc<Settings>) -> bool {
    for rule in settings.rules.iter() {
        if let Some(expression) = &rule.jmespath {
            if !map.contains_key(expression) {
                if let Ok(compiled) = jmespath::compile(expression) {
                    map.insert(expression.to_string(), compiled);
                } else {
                    error!("Failed to compile the JMESPath expression: {}", expression);
                    return false;
                }
            }
        }
    }
    true
}

/// perform_merge will generate the buffer resulting of the JSON merge
fn perform_merge(buffer: &mut str, template_id: &str, state: &RuleState) -> Result<String, String> {
    if let Ok(mut msg_json) = crate::json::from_str(buffer) {
        if let Ok(mut rendered) = state.hb.render(template_id, &state.variables) {
            let to_merge: serde_json::Value = crate::json::from_str(&mut rendered)
                .expect("Failed to deserialize our rendered to_merge_str");

            /*
             * If the administrator configured the merge incorrectly, just pass the buffer along un-merged
             */
            if !to_merge.is_object() {
                error!("Merge requested was not a JSON object: {}", to_merge);
                let _ = state.stats.send((Stats::MergeTargetNotJsonError, 1));
                return Ok(buffer.to_string());
            }

            merge::merge(&mut msg_json, &to_merge);

            if let Ok(output) = crate::json::to_string(&msg_json) {
                return Ok(output);
            }
        }
        Err("Failed to merge and serialize".to_string())
    } else {
        error!("Failed to parse as JSON, stopping actions: {}", buffer);
        let _ = state.stats.send((Stats::MergeInvalidJsonError, 1));
        Err("Not JSON".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_channel::bounded;

    /// Generating a test RuleState for consistent states in test
    fn rule_state<'a>(
        hb: &'a handlebars::Handlebars<'a>,
        hash: &'a HashMap<String, String>,
    ) -> RuleState<'a> {
        let (unused_sender, _) = bounded(1);
        RuleState {
            hb: &hb,
            variables: &hash,
            stats: unused_sender,
        }
    }

    #[test]
    fn merge_with_empty() {
        let mut hb = Handlebars::new();
        let template_id = "1";
        let _ = hb.register_template_string(template_id, "{}");

        let hash = HashMap::<String, String>::new();
        let state = rule_state(&hb, &hash);

        let mut buffer = "{}".to_string();
        let output = perform_merge(&mut buffer, template_id, &state);
        assert_eq!(output, Ok("{}".to_string()));
    }

    /// merge without a JSON object, this should return the original buffer
    #[test]
    fn merge_with_non_object() -> std::result::Result<(), String> {
        let mut hb = Handlebars::new();
        let template_id = "1";
        let _ = hb.register_template_string(template_id, "[1]");

        let hash = HashMap::<String, String>::new();
        let state = rule_state(&hb, &hash);

        let mut buffer = "{}".to_string();
        let output = perform_merge(&mut buffer, template_id, &state)?;
        assert_eq!(output, "{}".to_string());
        Ok(())
    }

    /// merging without a JSON buffer should return an error
    #[test]
    fn merge_without_json_buffer() {
        let mut hb = Handlebars::new();
        let template_id = "1";
        let _ = hb.register_template_string(template_id, "{}");

        let hash = HashMap::<String, String>::new();
        let state = rule_state(&hb, &hash);

        let mut buffer = "invalid".to_string();
        let output = perform_merge(&mut buffer, template_id, &state);
        let expected = Err("Not JSON".to_string());
        assert_eq!(output, expected);
    }

    /// merging with a JSON buffer should return Ok with the right result
    #[test]
    fn merge_with_json_buffer() {
        let mut hb = Handlebars::new();
        let template_id = "1";
        let _ = hb.register_template_string(template_id, r#"{"hello":1}"#);

        let hash = HashMap::<String, String>::new();
        let state = rule_state(&hb, &hash);

        let mut buffer = "{}".to_string();
        let output = perform_merge(&mut buffer, template_id, &state);
        assert_eq!(output, Ok("{\"hello\":1}".to_string()));
    }

    /// Ensure that merging with a JSON buffer that it renders variable substitutions
    #[test]
    fn merge_with_json_buffer_and_vars() {
        let mut hb = Handlebars::new();
        let template_id = "1";
        let _ = hb.register_template_string(template_id, r#"{"hello":"{{name}}"}"#);

        let mut hash = HashMap::<String, String>::new();
        hash.insert("name".to_string(), "world".to_string());
        let state = rule_state(&hb, &hash);

        let mut buffer = "{}".to_string();
        let output = perform_merge(&mut buffer, template_id, &state);
        assert_eq!(output, Ok("{\"hello\":\"world\"}".to_string()));
    }

    #[test]
    fn test_precompile_templates_merge() {
        let mut hb = Handlebars::new();
        let settings = Arc::new(load("test/configs/single-rule-with-merge.yml"));
        // Assuming that we're going to register the template with this id
        let template_id = format!("{}-{}", settings.rules[0].uuid, 0);

        let result = precompile_templates(&mut hb, settings.clone());
        assert!(result);
        assert!(hb.has_template(&template_id));
    }

    #[test]
    fn test_precompile_templates_replace() {
        let mut hb = Handlebars::new();
        let settings = Arc::new(load("test/configs/single-rule-with-replace.yml"));
        // Assuming that we're going to register the template with this id
        let template_id = format!("{}-{}", settings.rules[0].uuid, 0);

        let result = precompile_templates(&mut hb, settings.clone());
        assert!(result);
        assert!(hb.has_template(&template_id));
    }

    #[test]
    fn test_precompile_jmespath() {
        let settings = Arc::new(load("test/configs/single-rule-with-merge.yml"));
        let mut map = JmesPathExpressions::new();
        let result = precompile_jmespath(&mut map, settings.clone());
        assert!(result);
        let expected = settings.rules[0].jmespath.as_ref().unwrap();
        assert!(map.contains_key(expected));
    }

    #[test]
    fn test_precompile_jmespath_baddata() {
        let settings = Arc::new(load("test/configs/single-rule-with-invalid-jmespath.yml"));
        let mut map = JmesPathExpressions::new();
        let result = precompile_jmespath(&mut map, settings.clone());
        assert!(!result);
    }
}
