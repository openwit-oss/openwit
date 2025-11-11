use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, KeyValue as ProtoKeyValue},
    resource::v1::Resource as OtlpResourceProto,
};
use std::time::UNIX_EPOCH;

// Helper to format nanosecond-precision Unix timestamps into human-readable strings
pub fn format_timestamp_ns(ns: u64) -> String {
    if ns == 0 {
        return "0 (timestamp not set or epoch)".to_string();
    }
    let secs = ns / 1_000_000_000;
    let nanos_remainder = (ns % 1_000_000_000) as u32;

    match UNIX_EPOCH.checked_add(std::time::Duration::new(secs, nanos_remainder)) {
        Some(system_time) => {
            let datetime: chrono::DateTime<chrono::Utc> = system_time.into();
            datetime.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true) // More precise
        }
        None => {
            // This case should be rare with valid u64 ns timestamps from epoch
            format!("{}ns (timestamp out of SystemTime representable range)", ns)
        }
    }
}

// Helper to format an OTLP AnyValue into a string
pub fn format_any_value(value_option: &Option<AnyValue>) -> String {
    match value_option {
        Some(any_value) => match &any_value.value {
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => {
                format!("\"{}\"", s.escape_debug()) // escape_debug for clarity
            }
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(b)) => b.to_string(),
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)) => i.to_string(),
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d)) => {
                // Simple float formatting: show .0 for whole numbers, otherwise full string
                if d.fract() == 0.0 {
                    format!("{:.1}", d) // e.g., 5.0
                } else {
                    d.to_string()     // e.g., 3.14159
                }
            }
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::ArrayValue(arr)) => {
                let values_str = arr
                    .values
                    .iter()
                    .map(|v_ref| format_any_value(&Some(v_ref.clone())))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("[{}]", values_str)
            }
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::KvlistValue(kv_list)) => {
                let kvs_str = kv_list
                    .values
                    .iter()
                    .map(|kv| format!("{}: {}", kv.key, format_any_value(&kv.value)))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{{{}}}", kvs_str)
            }
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BytesValue(b)) => {
                // Print first few bytes as hex, then length, to avoid huge outputs
                let max_bytes_to_print = 8;
                if b.len() > max_bytes_to_print {
                    let hex_part = b[..max_bytes_to_print]
                        .iter()
                        .map(|byte| format!("{:02x}", byte))
                        .collect::<String>();
                    format!("bytes[{}...](len:{})", hex_part, b.len())
                } else {
                    format!(
                        "bytes[{}]",
                        b.iter().map(|byte| format!("{:02x}", byte)).collect::<String>()
                    )
                }
            }
            None => "[empty_value_field]".to_string(),
        },
        None => "[no_value_wrapper]".to_string(),
    }
}

// Helper to format a slice of OTLP KeyValue attributes
pub fn format_attributes_indent(attributes: &[ProtoKeyValue], base_indent: &str) -> String {
    if attributes.is_empty() {
        return format!("{}Attributes: (none)\n", base_indent);
    }
    let mut s = format!("{}Attributes:\n", base_indent);
    let item_indent = format!("{}{}  - ", base_indent, base_indent.chars().next().unwrap_or(' ')); // crude indent
    for attr in attributes {
        s.push_str(&format!(
            "{}{}: {}\n",
            item_indent,
            attr.key,
            format_any_value(&attr.value)
        ));
    }
    s
}

// Helper to format an OTLP Resource
pub fn format_resource(resource_option: &Option<OtlpResourceProto>, base_indent: &str) -> String {
    let mut s = String::new();
    if let Some(resource) = resource_option {
        s.push_str(&format!("{}Resource:\n", base_indent));
        let item_indent = format!("{}{}  - ", base_indent, base_indent.chars().next().unwrap_or(' '));
        if resource.attributes.is_empty() && resource.dropped_attributes_count == 0 {
            s.push_str(&format!("{}  (attributes not set or empty)\n", base_indent));
        } else {
            for attr in &resource.attributes {
                s.push_str(&format!(
                    "{}{}: {}\n",
                    item_indent,
                    attr.key,
                    format_any_value(&attr.value)
                ));
            }
            if resource.dropped_attributes_count > 0 {
                s.push_str(&format!(
                    "{}  (Dropped Resource Attributes Count: {})\n",
                    base_indent,
                    resource.dropped_attributes_count
                ));
            }
        }
    } else {
        s.push_str(&format!("{}Resource: (not set)\n", base_indent));
    }
    s
}

// Simple float precision helper (placeholder for a more robust one if needed)
// For the `d.to_string()` part in format_any_value, if you want specific float formatting.
// This is a very basic example. Libraries like `ryu` or `dtoa` are for high-performance.
// Or just stick with d.to_string() for simplicity.
mod precision { // "humanities" in Hindi, just a namespace for fun
    #[allow(dead_code)]
    pub fn precision(_d: &f64) -> usize {
        // A more sophisticated version would check the number of decimal places.
        // For now, let's say 6 for general display.
        6
    }
}
