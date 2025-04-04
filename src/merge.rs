// Disabling this lint because I don't want to fix it in this imported code
#![allow(clippy::clone_double_ref)]
/**
 * This module is from the crate json_value_merge
 *  <https://github.com/jmfiaschi/json_value_merge/>
 *
 * It is licensed under the MIT license
 */
use serde_json::{Map, Value};

/// Trait used to merge Json Values
pub trait Merge {
    /// Method use to merge two Json Values : ValueA <- ValueB.
    ///
    /// # Examples:
    /// ```ignore
    /// use serde_json::Value;
    ///
    /// let mut first_json_value: Value = serde_json::from_str(r#"["a","b"]"#).unwrap();
    /// let secound_json_value: Value = serde_json::from_str(r#"["b","c"]"#).unwrap();
    /// first_json_value.merge(secound_json_value);
    /// assert_eq!(r#"["a","b","c"]"#, first_json_value.to_string());
    ///
    /// OR
    ///
    /// let mut first_json_value: Value =
    /// serde_json::from_str(r#"{"value1":"a","value2":"b"}"#).unwrap();
    /// let secound_json_value: Value =
    ///     serde_json::from_str(r#"{"value1":"a","value2":"c","value3":"d"}"#).unwrap();
    /// first_json_value.merge(secound_json_value);
    /// assert_eq!(
    ///     r#"{"value1":"a","value2":"c","value3":"d"}"#,
    ///     first_json_value.to_string()
    /// );
    /// ```
    fn merge(&mut self, new_json_value: Value);
    /// Merge a new value in specific json pointer
    ///
    /// # Examples:
    /// ```ignore
    /// use serde_json::Value;
    ///
    /// let mut value_a: Value = serde_json::from_str(r#"{"my_array":[{"a":"t"}]}"#).unwrap();
    /// let value_b: Value = serde_json::from_str(r#"["b","c"]"#).unwrap();
    /// value_a.merge_in("/my_array", value_b.clone());
    /// assert_eq!(r#"{"my_array":[{"a":"t"},"b","c"]}"#, value_a.to_string());
    ///
    /// OR
    ///
    /// let mut value_a: Value = serde_json::from_str(r#"{"my_array":[{"a":"t"}]}"#).unwrap();
    /// let value_b: Value = serde_json::from_str(r#"{"b":"c"}"#).unwrap();
    /// value_a.merge_in("/my_array/0/a", value_b.clone());
    /// assert_eq!(r#"{"my_array":[{"a":{"b":"c"}}]}"#, value_a.to_string());
    /// ```
    fn merge_in(&mut self, json_pointer: &str, new_json_value: Value);
}

impl Merge for serde_json::Value {
    fn merge(&mut self, new_json_value: Value) {
        merge(self, &new_json_value);
    }
    fn merge_in(&mut self, json_pointer: &str, new_json_value: Value) {
        merge_in(self, json_pointer, new_json_value);
    }
}

pub fn merge(a: &mut Value, b: &Value) {
    match (a, b) {
        (&mut Value::Object(ref mut a), &Value::Object(ref b)) => {
            for (k, v) in b {
                merge(a.entry(k.clone()).or_insert(Value::Null), v);
            }
        }
        (&mut Value::Array(ref mut a), &Value::Array(ref b)) => {
            a.extend(b.clone());
            a.dedup();
        }
        (&mut Value::Array(ref mut a), &Value::Object(ref b)) => {
            a.push(Value::Object(b.clone()));
            a.dedup();
        }
        (a, b) => {
            *a = b.clone();
        }
    }
}

fn merge_in(json_value: &mut Value, json_pointer: &str, new_json_value: Value) {
    let mut fields: Vec<&str> = json_pointer.split('/').skip(1).collect();
    let first_field = fields[0];
    fields.remove(0);
    let next_fields = fields;

    // if json_pointer = "/"
    if first_field.is_empty() {
        json_value.merge(new_json_value);
        return;
    }

    match json_value.pointer_mut(format!("/{}", first_field).as_str()) {
        // Find the field and the json_value_targeted.
        Some(json_targeted) => {
            if !next_fields.is_empty() {
                merge_in(
                    json_targeted,
                    format!("/{}", next_fields.join("/")).as_ref(),
                    new_json_value,
                );
            } else {
                json_targeted.merge(new_json_value);
            }
        }
        // Not find the field and the json_value_targeted.
        // Add the new field and retry the merge_in with same parameters.
        None => {
            let new_value = match first_field.parse::<usize>().ok() {
                Some(position) => {
                    let mut vec = Vec::default();
                    match vec.get(position) {
                        Some(_) => vec.insert(position, Value::default()),
                        None => vec.push(Value::default()),
                    }
                    Value::Array(vec)
                }
                None => {
                    let mut map = Map::default();
                    map.insert(first_field.to_string(), Value::default());
                    Value::Object(map)
                }
            };
            json_value.merge(new_value);
            merge_in(json_value, json_pointer, new_json_value);
        }
    };
}

#[cfg(test)]
mod serde_json_value_updater_test {
    use super::*;
    #[test]
    fn it_should_merge_array_string() {
        let mut first_json_value: Value = serde_json::from_str(r#"["a","b"]"#).unwrap();
        let secound_json_value: Value = serde_json::from_str(r#"["b","c"]"#).unwrap();
        first_json_value.merge(secound_json_value);
        assert_eq!(r#"["a","b","c"]"#, first_json_value.to_string());
    }
    #[test]
    fn it_should_merge_array_object() {
        let mut first_json_value: Value =
            serde_json::from_str(r#"[{"value":"a"},{"value":"b"}]"#).unwrap();
        let secound_json_value: Value =
            serde_json::from_str(r#"[{"value":"b"},{"value":"c"}]"#).unwrap();
        first_json_value.merge(secound_json_value);
        assert_eq!(
            r#"[{"value":"a"},{"value":"b"},{"value":"c"}]"#,
            first_json_value.to_string()
        );
    }
    #[test]
    fn it_should_merge_object() {
        let mut first_json_value: Value =
            serde_json::from_str(r#"{"value1":"a","value2":"b"}"#).unwrap();
        let secound_json_value: Value =
            serde_json::from_str(r#"{"value1":"a","value2":"c","value3":"d"}"#).unwrap();
        first_json_value.merge(secound_json_value);
        assert_eq!(
            r#"{"value1":"a","value2":"c","value3":"d"}"#,
            first_json_value.to_string()
        );
    }
    #[test]
    fn it_should_merge_string() {
        let mut value_a: Value = Value::String("a".to_string());
        let value_b: Value = Value::String("b".to_string());
        value_a.merge(value_b.clone());
        assert_eq!(value_b.to_string(), value_a.to_string());
    }
    #[test]
    fn it_should_merge_an_array_in_a_specifique_field_path() {
        let mut value_a: Value = serde_json::from_str(r#"{"my_array":[{"a":"t"}]}"#).unwrap();
        let value_b: Value = serde_json::from_str(r#"["b","c"]"#).unwrap();
        value_a.merge_in("/my_array", value_b.clone());
        assert_eq!(r#"{"my_array":[{"a":"t"},"b","c"]}"#, value_a.to_string());
    }
    #[test]
    fn it_should_merge_an_object_in_a_specifique_field_path() {
        let mut value_a: Value = serde_json::from_str(r#"{"my_array":[{"a":"t"}]}"#).unwrap();
        let value_b: Value = serde_json::from_str(r#"{"b":"c"}"#).unwrap();
        value_a.merge_in("/my_array", value_b.clone());
        assert_eq!(r#"{"my_array":[{"a":"t"},{"b":"c"}]}"#, value_a.to_string());
    }
    #[test]
    fn it_should_merge_in_an_object_in_specifique_path_position() {
        let mut value_a: Value = serde_json::from_str(r#"{"my_array":[{"a":"t"}]}"#).unwrap();
        let value_b: Value = serde_json::from_str(r#"{"b":"c"}"#).unwrap();
        value_a.merge_in("/my_array/0", value_b.clone());
        assert_eq!(r#"{"my_array":[{"a":"t","b":"c"}]}"#, value_a.to_string());
    }
    #[test]
    fn it_should_merge_an_array_in_specifique_path_position() {
        let mut value_a: Value = serde_json::from_str(r#"{"my_array":[{"a":"t"}]}"#).unwrap();
        let value_b: Value = serde_json::from_str(r#"{"b":"c"}"#).unwrap();
        value_a.merge_in("/my_array/1", value_b.clone());
        assert_eq!(r#"{"my_array":[{"a":"t"},{"b":"c"}]}"#, value_a.to_string());
    }
    #[test]
    fn it_should_build_new_object() {
        let mut object: Value = Value::default();
        object.merge_in("/field", Value::String("value".to_string()));
        object.merge_in("/object", Value::Object(Map::default()));
        object.merge_in("/array", Value::Array(Vec::default()));
        assert_eq!(
            r#"{"array":[],"field":"value","object":{}}"#,
            object.to_string()
        );
    }
    #[test]
    fn it_should_merge_in_root_array() {
        let mut json_value: Value = serde_json::from_str(r#"["value"]"#).unwrap();
        let json_value_to_merge: Value = serde_json::from_str(r#"["new_value"]"#).unwrap();
        json_value.merge_in("/", json_value_to_merge);
        assert_eq!(r#"["value","new_value"]"#, json_value.to_string());
    }
    #[test]
    fn it_should_merge_in_root_object() {
        let mut json_value: Value = serde_json::from_str(r#"{"field":"value"}"#).unwrap();
        let json_value_to_merge: Value = serde_json::from_str(r#"{"field2":"value2"}"#).unwrap();
        json_value.merge_in("/", json_value_to_merge);
        assert_eq!(
            r#"{"field":"value","field2":"value2"}"#,
            json_value.to_string()
        );
    }
}
