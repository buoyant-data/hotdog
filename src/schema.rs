//!
//! Simple schema mappings to help turn defined schemas into Arrow schemas

use arrow_schema::DataType;
use serde::Deserialize;
use tracing::log::*;

use std::collections::HashMap;

/// Supported schema definition types for the configuration of hotdog
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    String,
    Struct,
    Long,
    Integer,
    Timestamp,
    Float,
    Boolean,
}

/// Helpful converter for [arrow_schema::Schema] conversion
impl Into<DataType> for &FieldType {
    fn into(self) -> DataType {
        match self {
            FieldType::String => DataType::Utf8,
            FieldType::Boolean => DataType::Boolean,
            FieldType::Integer => DataType::Int32,
            FieldType::Long => DataType::Int64,
            FieldType::Float => DataType::Float64,
            FieldType::Timestamp => DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
            unknown => {
                warn!(
                    "Asked to convert a schema type which cannot be automatically converted: {unknown:?}"
                );
                DataType::Null
            }
        }
    }
}

/// A field in the schema, which supports recursion. The name of the field is expected to be known
/// by the container of the field
#[derive(Clone, Debug, Deserialize)]
pub struct Field {
    r#type: FieldType,
    fields: Option<HashMap<String, Field>>,
}

impl Field {
    /// Retrieve the given [FieldType]
    pub fn get_type(&self) -> &FieldType {
        &self.r#type
    }

    /// Retrieve the nested fields should they exist
    pub fn fields(&self) -> Option<&HashMap<String, Field>> {
        self.fields.as_ref()
    }
}

/// Convert the given set of fields to an [arrow_schema::Schema] for use elsewher einside of hotdog
pub fn into_arrow_schema(
    fields: &HashMap<String, Field>,
) -> Result<arrow_schema::Schema, arrow_schema::ArrowError> {
    let mut arrow_fields = vec![];

    for (name, field) in fields {
        if field.get_type() == &FieldType::Struct {
            if let Some(fields) = field.fields.as_ref() {
                let struct_fields = into_arrow_schema(fields)?;
                arrow_fields.push(arrow_schema::Field::new(
                    name,
                    DataType::Struct(struct_fields.fields),
                    true,
                ));
            } else {
                error!(
                    "I don't know how to handle structs without fields! Calling it null :shrug:"
                );
                arrow_fields.push(arrow_schema::Field::new(name, DataType::Null, true));
            }
        } else {
            arrow_fields.push(arrow_schema::Field::new(
                name,
                field.get_type().into(),
                true,
            ));
        }
    }

    Ok(arrow_schema::Schema::new(arrow_fields))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deser_simple_field() {
        let buf = r#"
---
version:
  type: string
"#;
        let fields: HashMap<String, Field> =
            serde_yaml::from_str(buf).expect("Failed to deserialize a simple string field");

        if let Some(field) = fields.get("version") {
            assert_eq!(field.get_type(), &FieldType::String);
        } else {
            panic!("Failed to get a version field");
        }
    }

    #[test]
    fn test_deser_nested_map() {
        let buf = r#"
---
meta:
  type: struct
  fields:
    version:
      type: string
"#;
        let fields: HashMap<String, Field> =
            serde_yaml::from_str(buf).expect("Failed to deserialize a nested field");

        if let Some(field) = fields.get("meta") {
            assert_eq!(field.get_type(), &FieldType::Struct);

            let nested = field
                .fields
                .as_ref()
                .expect("Failed to get the nested fields");
            assert_eq!(
                nested.get("version").unwrap().get_type(),
                &FieldType::String
            );
        } else {
            panic!("Failed to get a timestamp field");
        }
    }

    /// The whole point of this schema definition is to get to an arrow schema definition!
    #[test]
    fn test_convert_simple_to_arrow() {
        let buf = r#"
---
version:
    type: string
"#;
        let fields: HashMap<String, Field> =
            serde_yaml::from_str(buf).expect("Failed to deserialize a nested field");

        let arr: arrow_schema::Schema =
            into_arrow_schema(&fields).expect("This is a valid arrow schema");
        assert_ne!(
            arr.fields.len(),
            0,
            "The converted schema should have one field"
        );
    }

    #[test]
    fn test_convert_nested_to_arrow() {
        let buf = r#"
---
meta:
  type: struct
  fields:
    version:
        type: string
"#;
        let fields: HashMap<String, Field> =
            serde_yaml::from_str(buf).expect("Failed to deserialize a nested field");

        let arr: arrow_schema::Schema =
            into_arrow_schema(&fields).expect("This is a valid arrow schema");

        let (_size, field) = arr
            .fields
            .find("meta")
            .expect("Failed to find the meta field");

        let struct_fields = arrow_schema::Fields::from(vec![arrow_schema::Field::new(
            "version",
            DataType::Utf8,
            true,
        )]);
        let struct_type = DataType::Struct(struct_fields);

        assert_eq!(field.data_type(), &struct_type);
    }
}
