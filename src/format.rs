use bytes::Bytes;
use chrono::{DateTime, Utc};
use clap::ValueEnum;
use serde_json::{json, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum InputFormat {
    Ndjson,
    Jsonl,
    Plain,
    Csv,
}

#[derive(Debug, Clone)]
pub struct FormatConfig {
    pub input_format: InputFormat,
    pub csv_has_headers: bool,
    pub trust_input: bool,
    pub csv_infer_types: bool,
    pub timestamp_field: Option<String>,
    pub timestamp_format: String,
    pub rename_fields: Vec<(String, String)>,
    pub drop_fields: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OversizedRecordPolicy {
    Fail,
    Dlq,
    Send,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum BadRecordPolicy {
    Fail,
    Skip,
    Dlq,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BadRecord {
    pub line_number: u64,
    pub start_offset: u64,
    pub end_offset: u64,
    pub reason: String,
    pub raw: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FormatOutcome {
    Emit(Bytes),
    Skip,
    Bad(BadRecord),
}

#[derive(Debug)]
pub struct RecordFormatter {
    config: FormatConfig,
    line_number: u64,
    csv_headers: Option<Vec<String>>,
}

impl RecordFormatter {
    pub fn new(config: FormatConfig) -> Self {
        Self {
            config,
            line_number: 0,
            csv_headers: None,
        }
    }

    pub fn format_line(&mut self, raw: Bytes, start_offset: u64, end_offset: u64) -> FormatOutcome {
        self.line_number += 1;
        let line_number = self.line_number;
        let outcome = match self.config.input_format {
            InputFormat::Ndjson | InputFormat::Jsonl if self.config.trust_input => {
                if raw.is_empty() {
                    FormatOutcome::Skip
                } else {
                    FormatOutcome::Emit(raw)
                }
            }
            InputFormat::Ndjson | InputFormat::Jsonl => {
                format_ndjson(&self.config, raw, line_number, start_offset, end_offset)
            }
            InputFormat::Plain => format_plain(raw),
            InputFormat::Csv => self.format_csv(raw, line_number, start_offset, end_offset),
        };
        apply_transforms(&self.config, outcome, line_number, start_offset, end_offset)
    }

    fn format_csv(
        &mut self,
        raw: Bytes,
        line_number: u64,
        start_offset: u64,
        end_offset: u64,
    ) -> FormatOutcome {
        format_csv_line(
            &self.config,
            &mut self.csv_headers,
            raw,
            line_number,
            start_offset,
            end_offset,
        )
    }
}

fn format_ndjson(
    config: &FormatConfig,
    raw: Bytes,
    line_number: u64,
    start_offset: u64,
    end_offset: u64,
) -> FormatOutcome {
    if raw.is_empty() {
        return FormatOutcome::Skip;
    }
    match parse_json_value(&raw) {
        Ok(Value::Object(mut map)) => {
            if let Err(reason) = normalize_timestamp(config, &mut map) {
                return FormatOutcome::Bad(BadRecord {
                    line_number,
                    start_offset,
                    end_offset,
                    reason,
                    raw,
                });
            }
            let mut value = Value::Object(map);
            apply_field_transforms(config, &mut value);
            let bytes =
                serde_json::to_vec(&value).expect("serializing transformed json cannot fail");
            FormatOutcome::Emit(Bytes::from(bytes))
        }
        Ok(_) => FormatOutcome::Bad(BadRecord {
            line_number,
            start_offset,
            end_offset,
            reason: "ndjson record must be a JSON object".to_string(),
            raw,
        }),
        Err(err) => FormatOutcome::Bad(BadRecord {
            line_number,
            start_offset,
            end_offset,
            reason: format!("invalid json: {err}"),
            raw,
        }),
    }
}

fn format_plain(raw: Bytes) -> FormatOutcome {
    if raw.is_empty() {
        return FormatOutcome::Skip;
    }
    let text = String::from_utf8_lossy(&raw);
    let bytes = serde_json::to_vec(&json!({ "message": text }))
        .expect("serializing plain text wrapper cannot fail");
    FormatOutcome::Emit(Bytes::from(bytes))
}

fn format_csv_line(
    config: &FormatConfig,
    headers: &mut Option<Vec<String>>,
    raw: Bytes,
    line_number: u64,
    start_offset: u64,
    end_offset: u64,
) -> FormatOutcome {
    let raw_text = String::from_utf8_lossy(&raw);
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(raw_text.as_bytes());
    let mut records = reader.records();
    let record = match records.next() {
        Some(Ok(record)) => record,
        Some(Err(err)) => {
            return FormatOutcome::Bad(BadRecord {
                line_number,
                start_offset,
                end_offset,
                reason: format!("invalid csv: {err}"),
                raw,
            });
        }
        None => return FormatOutcome::Skip,
    };

    if config.csv_has_headers && headers.is_none() {
        *headers = Some(record.iter().map(ToOwned::to_owned).collect());
        return FormatOutcome::Skip;
    }

    let value = if let Some(headers) = headers.as_ref() {
        if headers.len() != record.len() {
            return FormatOutcome::Bad(BadRecord {
                line_number,
                start_offset,
                end_offset,
                reason: format!(
                    "csv field count {} does not match header count {}",
                    record.len(),
                    headers.len()
                ),
                raw,
            });
        }
        let mut map = serde_json::Map::new();
        for (header, field) in headers.iter().zip(record.iter()) {
            map.insert(
                header.clone(),
                infer_or_string(field, config.csv_infer_types),
            );
        }
        Value::Object(map)
    } else {
        let fields: Vec<Value> = record
            .iter()
            .map(|field| infer_or_string(field, config.csv_infer_types))
            .collect();
        json!({ "fields": fields })
    };

    let bytes = serde_json::to_vec(&value).expect("serializing csv wrapper cannot fail");
    FormatOutcome::Emit(Bytes::from(bytes))
}
fn parse_json_value(raw: &[u8]) -> serde_json::Result<Value> {
    #[cfg(feature = "simd-json-validation")]
    {
        let mut owned = raw.to_vec();
        return simd_json::serde::from_slice(&mut owned).map_err(|err| {
            serde_json::Error::io(std::io::Error::new(std::io::ErrorKind::InvalidData, err))
        });
    }
    #[cfg(not(feature = "simd-json-validation"))]
    {
        serde_json::from_slice(raw)
    }
}

fn infer_or_string(field: &str, infer: bool) -> Value {
    if infer {
        infer_csv_value(field)
    } else {
        Value::String(field.to_string())
    }
}

fn infer_csv_value(field: &str) -> Value {
    if field.is_empty() {
        return Value::Null;
    }
    if let Ok(value) = field.parse::<i64>() {
        return Value::Number(value.into());
    }
    if let Ok(value) = field.parse::<f64>() {
        if let Some(number) = serde_json::Number::from_f64(value) {
            return Value::Number(number);
        }
    }
    if field.eq_ignore_ascii_case("true") {
        return Value::Bool(true);
    }
    if field.eq_ignore_ascii_case("false") {
        return Value::Bool(false);
    }
    Value::String(field.to_string())
}

fn apply_transforms(
    config: &FormatConfig,
    outcome: FormatOutcome,
    line_number: u64,
    start_offset: u64,
    end_offset: u64,
) -> FormatOutcome {
    let FormatOutcome::Emit(bytes) = outcome else {
        return outcome;
    };
    if config.timestamp_field.is_none()
        && config.rename_fields.is_empty()
        && config.drop_fields.is_empty()
    {
        return FormatOutcome::Emit(bytes);
    }
    let mut value = match serde_json::from_slice::<Value>(&bytes) {
        Ok(Value::Object(map)) => Value::Object(map),
        Ok(_) => return FormatOutcome::Emit(bytes),
        Err(err) => {
            return FormatOutcome::Bad(BadRecord {
                line_number,
                start_offset,
                end_offset,
                reason: format!("invalid transformed json: {err}"),
                raw: bytes,
            });
        }
    };
    if let Value::Object(map) = &mut value {
        if let Err(reason) = normalize_timestamp(config, map) {
            return FormatOutcome::Bad(BadRecord {
                line_number,
                start_offset,
                end_offset,
                reason,
                raw: bytes,
            });
        }
    }
    apply_field_transforms(config, &mut value);
    let bytes = serde_json::to_vec(&value).expect("serializing transformed object cannot fail");
    FormatOutcome::Emit(Bytes::from(bytes))
}

fn normalize_timestamp(
    config: &FormatConfig,
    map: &mut serde_json::Map<String, Value>,
) -> Result<(), String> {
    let Some(field) = &config.timestamp_field else {
        return Ok(());
    };
    let Some(value) = map.get_mut(field) else {
        return Ok(());
    };
    let normalized = normalize_timestamp_value(value)
        .ok_or_else(|| format!("timestamp field {field} could not be normalized"))?;
    *value = Value::String(normalized);
    Ok(())
}

fn normalize_timestamp_value(value: &Value) -> Option<String> {
    match value {
        Value::Number(number) => {
            let raw = number.as_i64()?;
            let millis = if raw.abs() >= 10_000_000_000 {
                raw
            } else {
                raw * 1000
            };
            DateTime::<Utc>::from_timestamp_millis(millis).map(|dt| dt.to_rfc3339())
        }
        Value::String(text) => {
            if let Ok(raw) = text.parse::<i64>() {
                let millis = if raw.abs() >= 10_000_000_000 {
                    raw
                } else {
                    raw * 1000
                };
                return DateTime::<Utc>::from_timestamp_millis(millis).map(|dt| dt.to_rfc3339());
            }
            DateTime::parse_from_rfc3339(text)
                .ok()
                .map(|dt| dt.with_timezone(&Utc).to_rfc3339())
        }
        _ => None,
    }
}

fn apply_field_transforms(config: &FormatConfig, value: &mut Value) {
    let Value::Object(map) = value else {
        return;
    };
    for field in &config.drop_fields {
        map.remove(field);
    }
    for (old, new) in &config.rename_fields {
        if old == new {
            continue;
        }
        if let Some(value) = map.remove(old) {
            map.insert(new.clone(), value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn formatter(input_format: InputFormat) -> RecordFormatter {
        RecordFormatter::new(FormatConfig {
            input_format,
            csv_has_headers: true,
            trust_input: false,
            csv_infer_types: false,
            timestamp_field: None,
            timestamp_format: "auto".to_string(),
            rename_fields: Vec::new(),
            drop_fields: Vec::new(),
        })
    }

    fn emitted_value(outcome: FormatOutcome) -> Value {
        match outcome {
            FormatOutcome::Emit(bytes) => serde_json::from_slice(&bytes).unwrap(),
            other => panic!("expected emitted record, got {other:?}"),
        }
    }

    #[test]
    fn ndjson_accepts_json_objects() {
        let mut formatter = formatter(InputFormat::Ndjson);
        let outcome = formatter.format_line(Bytes::from_static(br#"{"a":1}"#), 0, 7);
        assert_eq!(
            outcome,
            FormatOutcome::Emit(Bytes::from_static(br#"{"a":1}"#))
        );
    }

    #[test]
    fn ndjson_rejects_invalid_json() {
        let mut formatter = formatter(InputFormat::Ndjson);
        let outcome = formatter.format_line(Bytes::from_static(b"not-json"), 0, 8);
        match outcome {
            FormatOutcome::Bad(record) => assert!(record.reason.contains("invalid json")),
            other => panic!("expected bad record, got {other:?}"),
        }
    }

    #[test]
    fn trust_input_skips_json_validation_for_ndjson() {
        let mut formatter = RecordFormatter::new(FormatConfig {
            input_format: InputFormat::Ndjson,
            csv_has_headers: true,
            trust_input: true,
            csv_infer_types: false,
            timestamp_field: None,
            timestamp_format: "auto".to_string(),
            rename_fields: Vec::new(),
            drop_fields: Vec::new(),
        });
        assert_eq!(
            formatter.format_line(Bytes::from_static(b"not-json"), 0, 8),
            FormatOutcome::Emit(Bytes::from_static(b"not-json"))
        );
    }

    #[test]
    fn plain_wraps_line_as_message_json() {
        let mut formatter = formatter(InputFormat::Plain);
        let value = emitted_value(formatter.format_line(Bytes::from_static(b"hello"), 0, 5));
        assert_eq!(value, json!({ "message": "hello" }));
    }

    #[test]
    fn csv_with_headers_wraps_records_as_objects() {
        let mut formatter = formatter(InputFormat::Csv);
        assert_eq!(
            formatter.format_line(Bytes::from_static(b"name,age"), 0, 9),
            FormatOutcome::Skip
        );
        let value = emitted_value(formatter.format_line(Bytes::from_static(b"ana,42"), 10, 16));
        assert_eq!(value, json!({ "name": "ana", "age": "42" }));
    }

    #[test]
    fn csv_type_inference_converts_common_values() {
        let mut formatter = RecordFormatter::new(FormatConfig {
            input_format: InputFormat::Csv,
            csv_has_headers: true,
            trust_input: false,
            csv_infer_types: true,
            timestamp_field: None,
            timestamp_format: "auto".to_string(),
            rename_fields: Vec::new(),
            drop_fields: Vec::new(),
        });
        assert_eq!(
            formatter.format_line(Bytes::from_static(b"count,enabled,empty,name"), 0, 24),
            FormatOutcome::Skip
        );
        let value =
            emitted_value(formatter.format_line(Bytes::from_static(b"42,true,,ana"), 25, 37));
        assert_eq!(
            value,
            json!({ "count": 42, "enabled": true, "empty": null, "name": "ana" })
        );
    }

    #[test]
    fn normalizes_timestamp_field_to_rfc3339() {
        let mut formatter = RecordFormatter::new(FormatConfig {
            input_format: InputFormat::Ndjson,
            csv_has_headers: true,
            trust_input: false,
            csv_infer_types: false,
            timestamp_field: Some("ts".to_string()),
            timestamp_format: "auto".to_string(),
            rename_fields: Vec::new(),
            drop_fields: Vec::new(),
        });
        let value = emitted_value(formatter.format_line(
            Bytes::from_static(br#"{"ts":1700000000}"#),
            0,
            17,
        ));
        assert_eq!(value, json!({ "ts": "2023-11-14T22:13:20+00:00" }));
    }

    #[test]
    fn renames_and_drops_object_fields() {
        let mut formatter = RecordFormatter::new(FormatConfig {
            input_format: InputFormat::Ndjson,
            csv_has_headers: true,
            trust_input: false,
            csv_infer_types: false,
            timestamp_field: None,
            timestamp_format: "auto".to_string(),
            rename_fields: vec![("old".to_string(), "new".to_string())],
            drop_fields: vec!["secret".to_string()],
        });
        let value = emitted_value(formatter.format_line(
            Bytes::from_static(br#"{"old":1,"secret":"x","keep":true}"#),
            0,
            34,
        ));
        assert_eq!(value, json!({ "new": 1, "keep": true }));
    }
    #[test]
    fn csv_without_headers_wraps_records_as_field_array() {
        let mut formatter = RecordFormatter::new(FormatConfig {
            input_format: InputFormat::Csv,
            csv_has_headers: false,
            trust_input: false,
            csv_infer_types: false,
            timestamp_field: None,
            timestamp_format: "auto".to_string(),
            rename_fields: Vec::new(),
            drop_fields: Vec::new(),
        });
        let value = emitted_value(formatter.format_line(Bytes::from_static(b"ana,42"), 0, 6));
        assert_eq!(value, json!({ "fields": ["ana", "42"] }));
    }
}
