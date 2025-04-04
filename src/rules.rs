//!
//! Rules processing
//!

use smol::fs::*;
use smol::io::{AsyncBufReadExt, BufReader};
use smol::stream::StreamExt;
use tracing::log::*;

use std::collections::HashMap;
use std::sync::Arc;

use crate::errors;
use crate::settings::*;

pub async fn test_rules(
    file_name: &str,
    settings: Arc<Settings>,
) -> Result<(), errors::HotdogError> {
    let file = File::open(file_name)
        .await
        .expect("Failed to open the file");
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    let mut number: u64 = 0;

    while let Some(line) = lines.next().await {
        let line = line?;
        debug!("Testing the line: {}", line);
        number += 1;
        let mut matches: Vec<&Rule> = vec![];
        let mut unused = HashMap::<String, String>::new();
        let also_unused = HashMap::<String, jmespath::Expression>::new();

        for rule in settings.rules.iter() {
            match rule.field {
                Field::Msg => {
                    if apply_rule(rule, &line, &also_unused, &mut unused) {
                        matches.push(rule);
                    }
                }
                _ => {
                    error!("The test mode will only work on `field: msg` rules");
                }
            }
        }

        if !matches.is_empty() {
            println!("Line {} matches on:", number);
            for m in matches.iter() {
                println!("\t - {}", m);
            }
        }
    }

    Ok(())
}
/**
 * Attempt to apply the given rule to the given field value, inserting the
 * necessary variables into the hash along the way.
 *
 * If the rule matches, then this will return true
 */
pub fn apply_rule(
    rule: &Rule,
    value: &str,
    jmespaths: &crate::connection::JmesPathExpressions,
    hash: &mut HashMap<String, String>,
) -> bool {
    let mut rule_matches = false;
    /*
     * Check to see if we have a jmespath first
     *
     */
    if let Some(expression) = &rule.jmespath {
        let expr = &jmespaths[expression];
        if let Ok(data) = jmespath::Variable::from_json(value) {
            // Search the data with the compiled expression
            if let Ok(result) = expr.search(data) {
                if !result.is_null() {
                    rule_matches = true;
                    debug!("jmespath rule matched, value: {}", result);
                    if let Some(value) = result.as_string() {
                        hash.insert("value".to_string(), value.to_string());
                    } else {
                        warn!(
                            "Unable to parse out the string value for {}, the `value` variable substitution will not be available,",
                            result
                        );
                    }
                }
            }
        }
    } else if let Some(regex) = &rule.regex {
        if let Some(captures) = regex.captures(value) {
            rule_matches = true;

            for name in regex.capture_names() {
                if let Some(name) = name {
                    if let Some(value) = captures.name(name) {
                        hash.insert(name.to_string(), String::from(value.as_str()));
                    }
                }
            }
        }
    }
    rule_matches
}
