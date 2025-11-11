use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    ArrayBuilder, ArrayRef, BooleanBuilder, Int32Builder, Int64Builder, ListBuilder, MapBuilder,
    StringBuilder, StructBuilder,
};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::record_batch::RecordBatch;

use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

#[derive(Debug, Clone)]
pub struct SpanEvent {
    pub event_name: String,
    pub event_timestamp_nanos: i64,
}

#[derive(Debug, Clone)]
pub struct SpanRow {
    pub trace_id: String,
    pub span_id: String,
    pub span_name: String,
    pub service_name: String,
    pub span_start_timestamp_nanos: i64,
    pub span_end_timestamp_nanos: i64,
    pub span_duration_millis: i64,
    pub span_kind: i32,
    pub is_root: bool,
    pub parent_span_id: Option<String>,
    pub resource_attributes: HashMap<String, String>,
    pub span_attributes: HashMap<String, String>,
    pub events: Vec<SpanEvent>,
}

pub struct ArrowMemTable {
    trace_id: StringBuilder,
    span_id: StringBuilder,
    span_name: StringBuilder,
    service_name: StringBuilder,
    span_start_timestamp_nanos: Int64Builder,
    span_end_timestamp_nanos: Int64Builder,
    span_duration_millis: Int64Builder,
    span_kind: Int32Builder,
    is_root: BooleanBuilder,
    parent_span_id: StringBuilder,
    resource_attr_builder: MapBuilder<StringBuilder, StringBuilder>,
    span_attr_builder: MapBuilder<StringBuilder, StringBuilder>,
    events_builder: ListBuilder<StructBuilder>,
    schema: Arc<Schema>,
}

impl ArrowMemTable {
    pub fn new() -> Self {
        // Map fields: Map<entries: Struct<key: Utf8, value: Utf8>>
        let entry_struct_fields = Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]);
        let map_field = |name| {
            Field::new(
                name,
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(entry_struct_fields.clone()),
                        false,
                    )),
                    false,
                ),
                false,
            )
        };

        let event_struct_fields = Fields::from(vec![
            Field::new("event_name", DataType::Utf8, false),
            Field::new("event_timestamp_nanos", DataType::Int64, false),
        ]);
        let event_struct_builder = StructBuilder::new(
            event_struct_fields.clone(),
            vec![
                Box::new(StringBuilder::new()),
                Box::new(Int64Builder::new()),
            ],
        );
        let events_builder = ListBuilder::new(event_struct_builder);

        let schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("span_id", DataType::Utf8, false),
            Field::new("span_name", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("span_start_timestamp_nanos", DataType::Int64, false),
            Field::new("span_end_timestamp_nanos", DataType::Int64, false),
            Field::new("span_duration_millis", DataType::Int64, false),
            Field::new("span_kind", DataType::Int32, false),
            Field::new("is_root", DataType::Boolean, false),
            Field::new("parent_span_id", DataType::Utf8, true),
            map_field("resource_attributes"),
            map_field("span_attributes"),
            Field::new(
                "events",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(event_struct_fields),
                    false,
                ))),
                false,
            ),
        ]));

        let resource_attr_builder =
            MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        let span_attr_builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());

        Self {
            trace_id: StringBuilder::new(),
            span_id: StringBuilder::new(),
            span_name: StringBuilder::new(),
            service_name: StringBuilder::new(),
            span_start_timestamp_nanos: Int64Builder::new(),
            span_end_timestamp_nanos: Int64Builder::new(),
            span_duration_millis: Int64Builder::new(),
            span_kind: Int32Builder::new(),
            is_root: BooleanBuilder::new(),
            parent_span_id: StringBuilder::new(),
            resource_attr_builder,
            span_attr_builder,
            events_builder,
            schema,
        }
    }

    pub fn push(&mut self, row: &SpanRow) {
        self.trace_id.append_value(&row.trace_id);
        self.span_id.append_value(&row.span_id);
        self.span_name.append_value(&row.span_name);
        self.service_name.append_value(&row.service_name);
        self.span_start_timestamp_nanos
            .append_value(row.span_start_timestamp_nanos);
        self.span_end_timestamp_nanos
            .append_value(row.span_end_timestamp_nanos);
        self.span_duration_millis
            .append_value(row.span_duration_millis);
        self.span_kind.append_value(row.span_kind);
        self.is_root.append_value(row.is_root);
        self.parent_span_id
            .append_option(row.parent_span_id.as_deref());

        let n = row.resource_attributes.len();
        {
            let keys = self.resource_attr_builder.keys();
            for k in row.resource_attributes.keys() {
                keys.append_value(k);
            }
        }
        {
            let values = self.resource_attr_builder.values();
            for v in row.resource_attributes.values() {
                values.append_value(v);
            }
        }
        let _ = self.resource_attr_builder.append(n > 0);

        let n = row.span_attributes.len();
        {
            let keys = self.span_attr_builder.keys();
            for k in row.span_attributes.keys() {
                keys.append_value(k);
            }
        }
        {
            let values = self.span_attr_builder.values();
            for v in row.span_attributes.values() {
                values.append_value(v);
            }
        }
        let _ = self.span_attr_builder.append(n > 0);

        let events_values = self.events_builder.values();
        let event_struct_builder = events_values
            .as_any_mut()
            .downcast_mut::<StructBuilder>()
            .expect("event struct");
        for event in &row.events {
            event_struct_builder
                .field_builder::<StringBuilder>(0)
                .expect("field")
                .append_value(&event.event_name);
            event_struct_builder
                .field_builder::<Int64Builder>(1)
                .expect("field")
                .append_value(event.event_timestamp_nanos);
            event_struct_builder.append(true);
        }
        self.events_builder.append(row.events.len() > 0);
    }

    pub fn flush(&mut self) -> Option<RecordBatch> {
        if self.trace_id.len() == 0 {
            return None;
        }
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(self.trace_id.finish()) as ArrayRef,
            Arc::new(self.span_id.finish()) as ArrayRef,
            Arc::new(self.span_name.finish()) as ArrayRef,
            Arc::new(self.service_name.finish()) as ArrayRef,
            Arc::new(self.span_start_timestamp_nanos.finish()) as ArrayRef,
            Arc::new(self.span_end_timestamp_nanos.finish()) as ArrayRef,
            Arc::new(self.span_duration_millis.finish()) as ArrayRef,
            Arc::new(self.span_kind.finish()) as ArrayRef,
            Arc::new(self.is_root.finish()) as ArrayRef,
            Arc::new(self.parent_span_id.finish()) as ArrayRef,
            Arc::new(self.resource_attr_builder.finish()) as ArrayRef,
            Arc::new(self.span_attr_builder.finish()) as ArrayRef,
            Arc::new(self.events_builder.finish()) as ArrayRef,
        ];
        Some(RecordBatch::try_new(self.schema.clone(), arrays).unwrap())
    }

    pub fn len(&self) -> usize {
        self.trace_id.len()
    }

    pub fn estimated_memory_mb(&self) -> f64 {
        (self.len() as f64 * 512.0) / (1024.0 * 1024.0)
    }
}

pub fn span_row_from_otel(
    resource_spans: &ResourceSpans,
    _scope_spans: &ScopeSpans,
    span: &Span,
) -> SpanRow {
    let trace_id = hex::encode(&span.trace_id);
    let span_id = hex::encode(&span.span_id);
    let parent_span_id = if span.parent_span_id.is_empty() {
        None
    } else {
        Some(hex::encode(&span.parent_span_id))
    };

    let mut resource_attributes = HashMap::new();
    if let Some(resource) = &resource_spans.resource {
        for attr in &resource.attributes {
            if let Some(v) = &attr.value {
                if let Some(val) = &v.value {
                    use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueKind;
                    let s = match val {
                        AnyValueKind::StringValue(s) => s.clone(),
                        AnyValueKind::BoolValue(b) => b.to_string(),
                        AnyValueKind::IntValue(i) => i.to_string(),
                        AnyValueKind::DoubleValue(f) => f.to_string(),
                        _ => "".to_string(),
                    };
                    resource_attributes.insert(attr.key.clone(), s);
                }
            }
        }
    }
    let mut span_attributes = HashMap::new();
    for attr in &span.attributes {
        if let Some(v) = &attr.value {
            if let Some(val) = &v.value {
                use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueKind;
                let s = match val {
                    AnyValueKind::StringValue(s) => s.clone(),
                    AnyValueKind::BoolValue(b) => b.to_string(),
                    AnyValueKind::IntValue(i) => i.to_string(),
                    AnyValueKind::DoubleValue(f) => f.to_string(),
                    _ => "".to_string(),
                };
                span_attributes.insert(attr.key.clone(), s);
            }
        }
    }

    let events = span
        .events
        .iter()
        .map(|ev| SpanEvent {
            event_name: ev.name.clone(),
            event_timestamp_nanos: ev.time_unix_nano as i64,
        })
        .collect();

    SpanRow {
        trace_id,
        span_id,
        span_name: span.name.clone(),
        service_name: resource_attributes
            .get("service.name")
            .cloned()
            .unwrap_or_else(|| "".to_string()),
        span_start_timestamp_nanos: span.start_time_unix_nano as i64,
        span_end_timestamp_nanos: span.end_time_unix_nano as i64,
        span_duration_millis: ((span.end_time_unix_nano as i64)
            - (span.start_time_unix_nano as i64))
            / 1_000_000,
        span_kind: span.kind as i32,
        is_root: parent_span_id.is_none(),
        parent_span_id,
        resource_attributes,
        span_attributes,
        events,
    }
}
