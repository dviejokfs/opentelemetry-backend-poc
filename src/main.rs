use axum::{
    body::Bytes,
    extract::{Json, Path, Query, State},
    http::{Request, StatusCode},
    routing::{get, post},
    Router,
};
use opentelemetry_proto::tonic::{
    collector::trace::v1::{trace_service_server::TraceServiceServer, ExportTraceServiceRequest},
    common::v1::any_value::Value,
};

use opentelemetry_proto::tonic::common::v1::KeyValue;
use opentelemetry_proto::tonic::logs::v1::{LogRecord, LogsData, ResourceLogs, ScopeLogs};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};
use tower_http::trace::TraceLayer;
use tracing::field::{self, Empty};
use tracing::{debug, error, info, warn};
use tracing_subscriber::layer::SubscriberExt;

// Custom wrapper structs that match OTLP JSON format
#[derive(Debug, Serialize, Deserialize)]
pub struct OtlpJsonRequest {
    #[serde(rename = "resourceLogs", default)]
    pub resource_logs: Vec<ResourceLogs>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OtlpLogRecord {
    pub time_unix_nano: String,
    pub severity_number: i32,
    pub severity_text: String,
    pub body: OtlpValue,
    pub attributes: Option<Vec<OtlpAttribute>>,
    pub trace_id: String,
    pub span_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OtlpValue {
    #[serde(rename = "stringValue", skip_serializing_if = "Option::is_none")]
    pub string_value: Option<String>,
    #[serde(rename = "intValue", skip_serializing_if = "Option::is_none")]
    pub int_value: Option<i64>,
    // Add other value types as needed
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OtlpAttribute {
    pub key: String,
    pub value: OtlpValue,
}
// Structures for OTLP data
#[derive(Debug, Serialize, Deserialize)]
struct LogData {
    timestamp: i64,
    trace_id: String,
    span_id: String,
    severity: String,
    body: String,
    attributes: Option<serde_json::Value>,
}

// State handler for your database
#[derive(Clone)]
struct AppState {
    db: std::sync::Arc<tokio::sync::Mutex<db::Database>>,
}

// Handler for receiving logs via HTTP
async fn receive_logs(
    State(state): State<AppState>,
    Json(payload): Json<OtlpJsonRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    tracing::info!("Received logs: {:?}", payload);

    let mut db = state.db.lock().await;

    for resource_logs in payload.resource_logs {
        // Convert resource attributes to a proper JSON object
        let resource_attrs = resource_logs.resource.map(|r| {
            let mut map = serde_json::Map::new();
            for attr in r.attributes {
                let value = match attr.value.unwrap().value.unwrap() {
                    Value::StringValue(s) => serde_json::Value::String(s),
                    Value::IntValue(i) => serde_json::Value::Number(i.into()),
                    Value::DoubleValue(d) => serde_json::Value::Number(
                        serde_json::Number::from_f64(d).unwrap_or(serde_json::Number::from(0)),
                    ),
                    Value::BoolValue(b) => serde_json::Value::Bool(b),
                    _ => continue,
                };
                map.insert(attr.key, value);
            }
            serde_json::Value::Object(map)
        });

        for scope_logs in resource_logs.scope_logs {
            // Store scope information as JSON if present
            let scope_info = scope_logs
                .scope
                .map(|s| serde_json::to_value(&s).unwrap_or_default());

            for log_record in scope_logs.log_records {
                // Convert log attributes to a proper JSON object
                let attributes = {
                    let mut map = serde_json::Map::new();
                    for attr in log_record.attributes {
                        let value = match attr.value.unwrap().value.unwrap() {
                            Value::StringValue(s) => serde_json::Value::String(s),
                            Value::IntValue(i) => serde_json::Value::Number(i.into()),
                            Value::DoubleValue(d) => serde_json::Value::Number(
                                serde_json::Number::from_f64(d)
                                    .unwrap_or(serde_json::Number::from(0)),
                            ),
                            Value::BoolValue(b) => serde_json::Value::Bool(b),
                            _ => continue,
                        };
                        map.insert(attr.key, value);
                    }
                    serde_json::Value::Object(map)
                };

                let body_value = log_record
                    .body
                    .and_then(|b| b.value)
                    .map(|v| match v {
                        Value::StringValue(s) => s,
                        Value::IntValue(i) => i.to_string(),
                        _ => String::from("unknown value type"),
                    })
                    .unwrap_or_default();

                let log_entry = db::DbLogRecord {
                    time_unix_nano: log_record.time_unix_nano,
                    observed_time_unix_nano: log_record.observed_time_unix_nano,
                    severity_number: log_record.severity_number,
                    severity_text: log_record.severity_text,
                    body: body_value,
                    attributes, // Use our converted attributes
                    trace_id: hex::encode(&log_record.trace_id),
                    span_id: hex::encode(&log_record.span_id),
                    flags: log_record.flags,
                    resource_attributes: resource_attrs.clone(),
                    scope_info: scope_info.clone(),
                };

                if let Err(e) = db.store_log(&log_entry) {
                    error!("Failed to store log: {}", e);
                    return Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string()));
                }
            }
        }
    }

    Ok(StatusCode::OK)
}

// Handler for querying logs
async fn query_logs(// State(state): State<AppState>,
) -> Result<Json<Vec<LogData>>, StatusCode> {
    debug!("Processing logs query request");

    // Implement query logic
    // Example: let logs = state.db.query_logs().await?;

    Ok(Json(vec![]))
}

// Health check endpoint
async fn health_check() -> StatusCode {
    StatusCode::OK
}

// Add these imports at the top
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

// Add this struct with the traces
#[derive(Debug, Serialize, Deserialize)]
pub struct OtlpTraceRequest {
    #[serde(rename = "resourceSpans", default)]
    pub resource_spans: Vec<ResourceSpans>,
}

// Add the traces handler
async fn receive_traces(
    State(state): State<AppState>,
    Json(payload): Json<OtlpTraceRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    tracing::info!("Received traces: {:?}", payload);

    let mut db = state.db.lock().await;

    for resource_spans in payload.resource_spans {
        // Convert resource attributes to a proper JSON object
        let resource_attrs = resource_spans.resource.map(|r| {
            let mut map = serde_json::Map::new();
            for attr in r.attributes {
                let value = match attr.value.unwrap().value.unwrap() {
                    Value::StringValue(s) => serde_json::Value::String(s),
                    Value::IntValue(i) => serde_json::Value::Number(i.into()),
                    Value::DoubleValue(d) => serde_json::Value::Number(
                        serde_json::Number::from_f64(d).unwrap_or(serde_json::Number::from(0)),
                    ),
                    Value::BoolValue(b) => serde_json::Value::Bool(b),
                    _ => continue,
                };
                map.insert(attr.key, value);
            }
            serde_json::Value::Object(map)
        });

        for scope_spans in resource_spans.scope_spans {
            let scope_info = scope_spans
                .scope
                .map(|s| serde_json::to_value(&s).unwrap_or_default());

            for span in scope_spans.spans {
                let attributes = {
                    let mut map = serde_json::Map::new();
                    for attr in span.attributes {
                        let value = match attr.value.unwrap().value.unwrap() {
                            Value::StringValue(s) => serde_json::Value::String(s),
                            Value::IntValue(i) => serde_json::Value::Number(i.into()),
                            Value::DoubleValue(d) => serde_json::Value::Number(
                                serde_json::Number::from_f64(d)
                                    .unwrap_or(serde_json::Number::from(0)),
                            ),
                            Value::BoolValue(b) => serde_json::Value::Bool(b),
                            _ => continue,
                        };
                        map.insert(attr.key, value);
                    }
                    serde_json::Value::Object(map)
                };

                let trace_entry = db::DbTraceRecord {
                    id: 0,
                    trace_id: hex::encode(&span.trace_id),
                    span_id: hex::encode(&span.span_id),
                    parent_span_id: if !span.parent_span_id.is_empty() {
                        Some(hex::encode(&span.parent_span_id))
                    } else {
                        None
                    },
                    name: span.name,
                    kind: span.kind as i32,
                    start_time_unix_nano: span.start_time_unix_nano,
                    end_time_unix_nano: span.end_time_unix_nano,
                    attributes,
                    resource_attributes: resource_attrs.clone(),
                    scope_info: scope_info.clone(),
                    status_code: span.status.clone().map(|s| s.code as i32),
                    status_message: span.status.clone().and_then(|s| {
                        if s.message.is_empty() {
                            None
                        } else {
                            Some(s.message)
                        }
                    }),
                    flags: span.flags,
                };

                if let Err(e) = db.store_trace(&trace_entry) {
                    error!("Failed to store trace: {}", e);
                    return Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string()));
                }
            }
        }
    }

    Ok(StatusCode::OK)
}

// Add these query endpoints
async fn query_spans_by_trace(
    State(state): State<AppState>,
    Path(trace_id): Path<String>,
) -> Result<Json<Vec<db::DbTraceRecord>>, (StatusCode, String)> {
    let db = state.db.lock().await;
    match db.query_spans_by_trace_id(&trace_id) {
        Ok(spans) => Ok(Json(spans)),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

async fn query_span_by_id(
    State(state): State<AppState>,
    Path((trace_id, span_id)): Path<(String, String)>,
) -> Result<Json<db::DbTraceRecord>, (StatusCode, String)> {
    let db = state.db.lock().await;
    match db.query_span_by_id(&trace_id, &span_id) {
        Ok(Some(span)) => Ok(Json(span)),
        Ok(None) => Err((StatusCode::NOT_FOUND, "Span not found".to_string())),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

// Add this struct for receiving spans
#[derive(Debug, Serialize, Deserialize)]
pub struct OtlpSpanRequest {
    #[serde(rename = "resourceSpans", default)]
    pub resource_spans: Vec<ResourceSpans>,
}

// Add the spans handler
async fn receive_spans(
    State(state): State<AppState>,
    Json(payload): Json<OtlpSpanRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    tracing::info!("Received spans: {:?}", payload);

    let mut db = state.db.lock().await;

    for resource_spans in payload.resource_spans {
        // Convert resource attributes
        let resource_attrs = resource_spans.resource.map(|r| {
            let mut map = serde_json::Map::new();
            for attr in r.attributes {
                let value = match attr.value.unwrap().value.unwrap() {
                    Value::StringValue(s) => serde_json::Value::String(s),
                    Value::IntValue(i) => serde_json::Value::Number(i.into()),
                    Value::DoubleValue(d) => serde_json::Value::Number(
                        serde_json::Number::from_f64(d).unwrap_or(serde_json::Number::from(0)),
                    ),
                    Value::BoolValue(b) => serde_json::Value::Bool(b),
                    _ => continue,
                };
                map.insert(attr.key, value);
            }
            serde_json::Value::Object(map)
        });

        for scope_spans in resource_spans.scope_spans {
            let scope_info = scope_spans
                .scope
                .map(|s| serde_json::to_value(&s).unwrap_or_default());

            for span in scope_spans.spans {
                let attributes = {
                    let mut map = serde_json::Map::new();
                    for attr in span.attributes {
                        let value = match attr.value.unwrap().value.unwrap() {
                            Value::StringValue(s) => serde_json::Value::String(s),
                            Value::IntValue(i) => serde_json::Value::Number(i.into()),
                            Value::DoubleValue(d) => serde_json::Value::Number(
                                serde_json::Number::from_f64(d)
                                    .unwrap_or(serde_json::Number::from(0)),
                            ),
                            Value::BoolValue(b) => serde_json::Value::Bool(b),
                            _ => continue,
                        };
                        map.insert(attr.key, value);
                    }
                    serde_json::Value::Object(map)
                };

                let trace_entry = db::DbTraceRecord {
                    id: 0,
                    trace_id: hex::encode(&span.trace_id),
                    span_id: hex::encode(&span.span_id),
                    parent_span_id: if !span.parent_span_id.is_empty() {
                        Some(hex::encode(&span.parent_span_id))
                    } else {
                        None
                    },
                    name: span.name,
                    kind: span.kind as i32,
                    start_time_unix_nano: span.start_time_unix_nano,
                    end_time_unix_nano: span.end_time_unix_nano,
                    attributes,
                    resource_attributes: resource_attrs.clone(),
                    scope_info: scope_info.clone(),
                    status_code: span.status.clone().map(|s| s.code as i32),
                    status_message: span.status.clone().and_then(|s| {
                        if s.message.is_empty() {
                            None
                        } else {
                            Some(s.message)
                        }
                    }),
                    flags: span.flags,
                };

                if let Err(e) = db.store_trace(&trace_entry) {
                    error!("Failed to store span: {}", e);
                    return Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string()));
                }
            }
        }
    }

    Ok(StatusCode::OK)
}

// Add query parameters
#[derive(Debug, Deserialize)]
struct RootTracesQuery {
    limit: Option<i64>,
    from_time: Option<u64>,
    to_time: Option<u64>,
}

async fn get_root_traces(
    State(state): State<AppState>,
    Query(params): Query<RootTracesQuery>,
) -> Result<Json<Vec<db::DbTraceRecord>>, (StatusCode, String)> {
    let db = state.db.lock().await;
    match db.query_root_traces(params.limit, params.from_time, params.to_time) {
        Ok(traces) => Ok(Json(traces)),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

// Add these new handler functions
async fn get_trace_by_id(
    State(state): State<AppState>,
    Path(trace_id): Path<String>,
) -> Result<Json<db::DbTraceRecord>, (StatusCode, String)> {
    let db = state.db.lock().await;
    match db.query_trace_by_id(&trace_id) {
        Ok(Some(trace)) => Ok(Json(trace)),
        Ok(None) => Err((StatusCode::NOT_FOUND, "Trace not found".to_string())),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

async fn get_spans_by_trace_id(
    State(state): State<AppState>,
    Path(trace_id): Path<String>,
) -> Result<Json<Vec<db::DbTraceRecord>>, (StatusCode, String)> {
    let db = state.db.lock().await;
    match db.query_spans_by_trace_id(&trace_id) {
        Ok(spans) => Ok(Json(spans)),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

#[tokio::main]
async fn main() {
    // Initialize tracing with a more detailed configuration
    tracing_subscriber::fmt()
        .with_target(false)
        .with_thread_ids(true)
        .with_level(true)
        .with_file(true)
        .with_line_number(true)
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    info!("Initializing server...");

    // Create data directory if it doesn't exist
    let data_dir = "data";
    std::fs::create_dir_all(data_dir).expect("Failed to create data directory");

    // Initialize database with proper path
    let db_path = format!("{}/logs.db", data_dir);
    let db = db::Database::new(&db_path).unwrap_or_else(|e| {
        error!("Failed to initialize database: {}", e);
        std::process::exit(1);
    });
    let db = std::sync::Arc::new(tokio::sync::Mutex::new(db));

    // Create app state
    let state = AppState { db };
    debug!("App state initialized");

    // Build the router
    let app = Router::new()
        .route("/v1/logs", post(receive_logs))
        .route("/v1/traces", post(receive_traces))
        .route("/v1/spans", post(receive_spans))
        .route("/v1/traces/roots", get(get_root_traces))
        .route("/v1/traces/:trace_id", get(get_trace_by_id))
        .route("/v1/traces/:trace_id/spans", get(get_spans_by_trace_id))
        .route("/v1/logs/query", get(query_logs))
        .route("/health", get(health_check))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &Request<_>| {
                    let start_time = Instant::now();
                    tracing::info_span!(
                        "http_request",
                        method = %request.method(),
                        path = %request.uri().path(),
                        start_time = field::debug(start_time),
                        latency = Empty,
                    )
                })
                .on_response(
                    |response: &axum::http::Response<_>,
                     latency: Duration,
                     span: &tracing::Span| {
                        let latency_ms = latency.as_secs_f64() * 1000.0;
                        span.record("latency", tracing::field::debug(latency_ms));
                        tracing::info!(
                            "Response generated with status: {:?}, latency: {:?}ms",
                            response.status().as_u16(),
                            latency_ms,
                        );
                    },
                ),
        )
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 4000));
    info!("Server starting on {}", addr);

    // Run the server
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    info!("Server is ready to accept connections");

    axum::serve(listener, app)
        .await
        .unwrap_or_else(|e| error!("Server error: {}", e));
}

// Database implementation example
mod db {
    use super::*;
    use rusqlite::{params, params_from_iter, Connection, OptionalExtension, Result, ToSql, Transaction};
    use std::path::Path;

    pub struct Database {
        conn: Connection,
    }

    #[derive(Debug)]
    pub struct DbLogRecord {
        pub time_unix_nano: u64,
        pub observed_time_unix_nano: u64,
        pub severity_number: i32,
        pub severity_text: String,
        pub body: String,
        pub attributes: serde_json::Value,
        pub trace_id: String,
        pub span_id: String,
        pub flags: u32,
        pub resource_attributes: Option<serde_json::Value>,
        pub scope_info: Option<serde_json::Value>,
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub struct DbTraceRecord {
        pub id: i64,
        pub trace_id: String,
        pub span_id: String,
        pub parent_span_id: Option<String>,
        pub name: String,
        pub kind: i32,
        pub start_time_unix_nano: u64,
        pub end_time_unix_nano: u64,
        pub attributes: serde_json::Value,
        pub resource_attributes: Option<serde_json::Value>,
        pub scope_info: Option<serde_json::Value>,
        pub status_code: Option<i32>,
        pub status_message: Option<String>,
        pub flags: u32,
    }

    impl Database {
        pub fn new(db_path: &str) -> Result<Self> {
            if let Some(parent) = Path::new(db_path).parent() {
                std::fs::create_dir_all(parent).unwrap();
            }

            let conn = Connection::open(db_path)?;

            // Create the logs table
            conn.execute(
                r#"
                CREATE TABLE IF NOT EXISTS logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    time_unix_nano TEXT NOT NULL,
                    observed_time_unix_nano TEXT NOT NULL,
                    severity_number INTEGER NOT NULL,
                    severity_text TEXT NOT NULL,
                    body TEXT NOT NULL,
                    trace_id TEXT NOT NULL,
                    span_id TEXT NOT NULL,
                    flags INTEGER NOT NULL DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                "#,
                [],
            )?;

            // Create the log attributes table
            conn.execute(
                r#"
                CREATE TABLE IF NOT EXISTS log_attributes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    key TEXT NOT NULL,
                    value_type TEXT NOT NULL,
                    string_value TEXT,
                    int_value INTEGER,
                    double_value REAL,
                    bool_value INTEGER,
                    UNIQUE(key, value_type, string_value, int_value, double_value, bool_value)
                )
                "#,
                [],
            )?;

            // Create the resource attributes table
            conn.execute(
                r#"
                CREATE TABLE IF NOT EXISTS resource_attributes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    key TEXT NOT NULL,
                    value_type TEXT NOT NULL,
                    string_value TEXT,
                    int_value INTEGER,
                    double_value REAL,
                    bool_value INTEGER,
                    UNIQUE(key, value_type, string_value, int_value, double_value, bool_value)
                )
                "#,
                [],
            )?;

            // Create the linking tables
            conn.execute(
                r#"
                CREATE TABLE IF NOT EXISTS logs_to_log_attrs (
                    log_id INTEGER NOT NULL,
                    attr_id INTEGER NOT NULL,
                    FOREIGN KEY(log_id) REFERENCES logs(id),
                    FOREIGN KEY(attr_id) REFERENCES log_attributes(id),
                    PRIMARY KEY(log_id, attr_id)
                )
                "#,
                [],
            )?;

            conn.execute(
                r#"
                CREATE TABLE IF NOT EXISTS logs_to_resource_attrs (
                    log_id INTEGER NOT NULL,
                    attr_id INTEGER NOT NULL,
                    FOREIGN KEY(log_id) REFERENCES logs(id),
                    FOREIGN KEY(attr_id) REFERENCES resource_attributes(id),
                    PRIMARY KEY(log_id, attr_id)
                )
                "#,
                [],
            )?;

            // Create indexes
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_logs_trace_id ON logs(trace_id)",
                [],
            )?;
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_logs_time ON logs(time_unix_nano)",
                [],
            )?;
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_log_attrs_key ON log_attributes(key)",
                [],
            )?;
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_resource_attrs_key ON resource_attributes(key)",
                [],
            )?;

            // Create the traces table
            conn.execute(
                r#"
                CREATE TABLE IF NOT EXISTS traces (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trace_id TEXT NOT NULL,
                    span_id TEXT NOT NULL,
                    parent_span_id TEXT,
                    name TEXT NOT NULL,
                    kind INTEGER NOT NULL,
                    start_time_unix_nano TEXT NOT NULL,
                    end_time_unix_nano TEXT NOT NULL,
                    status_code INTEGER,
                    status_message TEXT,
                    flags INTEGER NOT NULL DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                "#,
                [],
            )?;

            // Create indexes for traces
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_traces_trace_id ON traces(trace_id)",
                [],
            )?;
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_traces_span_id ON traces(span_id)",
                [],
            )?;
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_traces_parent_span_id ON traces(parent_span_id)",
                [],
            )?;

            // Create span attributes tables
            conn.execute(
                r#"
                CREATE TABLE IF NOT EXISTS span_attributes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    key TEXT NOT NULL,
                    value_type TEXT NOT NULL,
                    string_value TEXT,
                    int_value INTEGER,
                    double_value REAL,
                    bool_value INTEGER,
                    UNIQUE(key, value_type, string_value, int_value, double_value, bool_value)
                )
                "#,
                [],
            )?;

            // Create span resource attributes tables
            conn.execute(
                r#"
                CREATE TABLE IF NOT EXISTS span_resource_attributes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    key TEXT NOT NULL,
                    value_type TEXT NOT NULL,
                    string_value TEXT,
                    int_value INTEGER,
                    double_value REAL,
                    bool_value INTEGER,
                    UNIQUE(key, value_type, string_value, int_value, double_value, bool_value)
                )
                "#,
                [],
            )?;

            // Create span attribute linking tables
            conn.execute(
                r#"
                CREATE TABLE IF NOT EXISTS spans_to_span_attrs (
                    span_id INTEGER NOT NULL,
                    attr_id INTEGER NOT NULL,
                    FOREIGN KEY(span_id) REFERENCES traces(id),
                    FOREIGN KEY(attr_id) REFERENCES span_attributes(id),
                    PRIMARY KEY(span_id, attr_id)
                )
                "#,
                [],
            )?;

            conn.execute(
                r#"
                CREATE TABLE IF NOT EXISTS spans_to_resource_attrs (
                    span_id INTEGER NOT NULL,
                    attr_id INTEGER NOT NULL,
                    FOREIGN KEY(span_id) REFERENCES traces(id),
                    FOREIGN KEY(attr_id) REFERENCES span_resource_attributes(id),
                    PRIMARY KEY(span_id, attr_id)
                )
                "#,
                [],
            )?;

            // Create indexes for span attributes
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_span_attrs_key ON span_attributes(key)",
                [],
            )?;
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_span_resource_attrs_key ON span_resource_attributes(key)",
                [],
            )?;

            Ok(Database { conn })
        }

        fn store_attribute(
            tx: &Transaction,
            log_id: i64,
            attr_type: &str,
            key: &str,
            value: &serde_json::Value,
        ) -> Result<()> {
            let (value_type, string_value, int_value, double_value, bool_value) = match value {
                serde_json::Value::String(s) => ("string", Some(s.as_str()), None, None, None),
                serde_json::Value::Number(n) if n.is_i64() => ("int", None, n.as_i64(), None, None),
                serde_json::Value::Number(n) => ("double", None, None, n.as_f64(), None),
                serde_json::Value::Bool(b) => ("bool", None, None, None, Some(*b)),
                _ => return Ok(()),
            };

            tx.execute(
                r#"
                INSERT INTO attributes (
                    log_id, attr_type, key, value_type, 
                    string_value, int_value, double_value, bool_value
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                "#,
                params![
                    log_id,
                    attr_type,
                    key,
                    value_type,
                    string_value,
                    int_value,
                    double_value,
                    bool_value.map(|b| b as i32),
                ],
            )?;

            Ok(())
        }

        pub fn query_logs_by_attribute(&self, key: &str, value: &str) -> Result<Vec<DbLogRecord>> {
            let mut stmt = self.conn.prepare(
                r#"
                SELECT DISTINCT l.* 
                FROM logs l
                JOIN log_attributes la ON l.id = la.log_id
                WHERE la.key = ?1 AND la.string_value = ?2
                ORDER BY l.time_unix_nano DESC
                "#,
            )?;

            let logs = stmt
                .query_map(params![key, value], |row| {
                    Ok(DbLogRecord {
                        time_unix_nano: row
                            .get::<_, String>("time_unix_nano")?
                            .parse()
                            .unwrap_or_default(),
                        observed_time_unix_nano: row
                            .get::<_, String>("observed_time_unix_nano")?
                            .parse()
                            .unwrap_or_default(),
                        severity_number: row.get("severity_number")?,
                        severity_text: row.get("severity_text")?,
                        body: row.get("body")?,
                        attributes: self.get_log_attributes(row.get("id")?).unwrap_or_default(),
                        trace_id: row.get("trace_id")?,
                        span_id: row.get("span_id")?,
                        flags: row.get("flags")?,
                        resource_attributes: Some(
                            self.get_resource_attributes(row.get("id")?)
                                .unwrap_or_default(),
                        ),
                        scope_info: row
                            .get::<_, Option<String>>("scope_info")?
                            .and_then(|s| serde_json::from_str(&s).ok()),
                    })
                })?
                .collect::<Result<Vec<_>>>()?;

            Ok(logs)
        }

        fn get_log_attributes(&self, log_id: i64) -> Result<serde_json::Value> {
            let mut stmt = self.conn.prepare(
                r#"
                SELECT key, value_type, string_value, int_value, double_value, bool_value
                FROM log_attributes
                WHERE log_id = ?1
                "#,
            )?;

            let mut map = serde_json::Map::new();
            let rows = stmt.query_map([log_id], |row| {
                let key: String = row.get("key")?;
                let value_type: String = row.get("value_type")?;
                let value = match value_type.as_str() {
                    "string" => serde_json::Value::String(row.get("string_value")?),
                    "int" => serde_json::Value::Number(row.get::<_, i64>("int_value")?.into()),
                    "double" => serde_json::Value::Number(
                        serde_json::Number::from_f64(row.get("double_value")?).unwrap(),
                    ),
                    "bool" => serde_json::Value::Bool(row.get::<_, i32>("bool_value")? != 0),
                    _ => serde_json::Value::Null,
                };
                Ok((key, value))
            })?;

            for row in rows {
                let (key, value) = row?;
                map.insert(key, value);
            }

            Ok(serde_json::Value::Object(map))
        }
        // Add this new method to query a single trace by ID
        pub fn query_trace_by_id(
            &self,
            trace_id: &str,
        ) -> Result<Option<db::DbTraceRecord>, rusqlite::Error> {
            let mut stmt = self.conn.prepare(
                r#"
        SELECT * FROM traces 
        WHERE trace_id = ?1 AND parent_span_id IS NULL
        LIMIT 1
        "#,
            )?;

            let mut traces = stmt
                .query_map([trace_id], |row| {
                    Ok(db::DbTraceRecord {
                        id: row.get("id")?,
                        trace_id: row.get("trace_id")?,
                        span_id: row.get("span_id")?,
                        parent_span_id: row.get("parent_span_id")?,
                        name: row.get("name")?,
                        kind: row.get("kind")?,
                        start_time_unix_nano: row
                            .get::<_, String>("start_time_unix_nano")?
                            .parse()
                            .unwrap_or_default(),
                        end_time_unix_nano: row
                            .get::<_, String>("end_time_unix_nano")?
                            .parse()
                            .unwrap_or_default(),
                        attributes: self
                            .get_span_attributes(row.get("id")?)
                            .unwrap_or_default(),
                        resource_attributes: Some(
                            self.get_span_resource_attributes(row.get("id")?)
                                .unwrap_or_default(),
                        ),
                        scope_info: None,
                        status_code: row.get("status_code")?,
                        status_message: row.get("status_message")?,
                        flags: row.get("flags")?,
                    })
                })?
                .collect::<Result<Vec<_>>>()?;

            Ok(traces.pop())
        }

        // Update the existing query_spans_by_trace_id method to include all spans
        pub fn query_spans_by_trace_id(
            &self,
            trace_id: &str,
        ) -> Result<Vec<db::DbTraceRecord>, rusqlite::Error> {
            let mut stmt = self.conn.prepare(
                r#"
        WITH RECURSIVE span_tree AS (
            -- Base case: get the root span
            SELECT *, 0 as depth
            FROM traces
            WHERE trace_id = ?1 AND parent_span_id IS NULL
            
            UNION ALL
            
            -- Recursive case: get all child spans
            SELECT t.*, st.depth + 1
            FROM traces t
            JOIN span_tree st ON t.parent_span_id = st.span_id
            WHERE t.trace_id = ?1
        )
        SELECT * FROM span_tree
        ORDER BY depth ASC, start_time_unix_nano ASC
        "#,
            )?;

            let spans = stmt
                .query_map([trace_id], |row| {
                    Ok(db::DbTraceRecord {
                        id: row.get("id")?,
                        trace_id: row.get("trace_id")?,
                        span_id: row.get("span_id")?,
                        parent_span_id: row.get("parent_span_id")?,
                        name: row.get("name")?,
                        kind: row.get("kind")?,
                        start_time_unix_nano: row
                            .get::<_, String>("start_time_unix_nano")?
                            .parse()
                            .unwrap_or_default(),
                        end_time_unix_nano: row
                            .get::<_, String>("end_time_unix_nano")?
                            .parse()
                            .unwrap_or_default(),
                        attributes: self
                            .get_span_attributes(row.get("id")?)
                            .unwrap_or_default(),
                        resource_attributes: Some(
                            self.get_span_resource_attributes(row.get("id")?)
                                .unwrap_or_default(),
                        ),
                        scope_info: None,
                        status_code: row.get("status_code")?,
                        status_message: row.get("status_message")?,
                        flags: row.get("flags")?,
                    })
                })?
                .collect::<Result<Vec<_>>>()?;

            Ok(spans)
        }
        fn get_resource_attributes(&self, log_id: i64) -> Result<serde_json::Value> {
            // Similar to get_log_attributes but for resource_attributes table
            let mut stmt = self.conn.prepare(
                r#"
                SELECT key, value_type, string_value, int_value, double_value, bool_value
                FROM resource_attributes
                WHERE log_id = ?1
                "#,
            )?;

            let mut map = serde_json::Map::new();
            let rows = stmt.query_map([log_id], |row| {
                let key: String = row.get("key")?;
                let value_type: String = row.get("value_type")?;
                let value = match value_type.as_str() {
                    "string" => serde_json::Value::String(row.get("string_value")?),
                    "int" => serde_json::Value::Number(row.get::<_, i64>("int_value")?.into()),
                    "double" => serde_json::Value::Number(
                        serde_json::Number::from_f64(row.get("double_value")?)
                            .unwrap_or(serde_json::Number::from(0)),
                    ),
                    "bool" => serde_json::Value::Bool(row.get::<_, i32>("bool_value")? != 0),
                    _ => serde_json::Value::Null,
                };
                Ok((key, value))
            })?;

            for row in rows {
                let (key, value) = row?;
                map.insert(key, value);
            }

            Ok(serde_json::Value::Object(map))
        }

        pub fn store_log(&mut self, log: &DbLogRecord) -> Result<()> {
            let tx = self.conn.transaction()?;

            // Insert main log record
            tx.execute(
                r#"
                INSERT INTO logs (
                    time_unix_nano, observed_time_unix_nano, severity_number, 
                    severity_text, body, trace_id, span_id, flags
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                "#,
                params![
                    log.time_unix_nano.to_string(),
                    log.observed_time_unix_nano.to_string(),
                    log.severity_number,
                    log.severity_text,
                    log.body,
                    log.trace_id,
                    log.span_id,
                    log.flags,
                ],
            )?;

            let log_id = tx.last_insert_rowid();

            // Helper function to store attributes
            fn store_attributes(
                tx: &Transaction,
                log_id: i64,
                attributes: &serde_json::Map<String, serde_json::Value>,
                table_name: &str,
                link_table: &str,
            ) -> Result<()> {
                for (key, value) in attributes {
                    // Insert or ignore the attribute
                    tx.execute(
                        &format!(
                            r#"
                            INSERT OR IGNORE INTO {} (
                                key, value_type, string_value, int_value, double_value, bool_value
                            )
                            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                            "#,
                            table_name
                        ),
                        params![
                            key,
                            match value {
                                serde_json::Value::String(_) => "string",
                                serde_json::Value::Number(n) if n.is_i64() => "int",
                                serde_json::Value::Number(_) => "double",
                                serde_json::Value::Bool(_) => "bool",
                                _ => continue,
                            },
                            match value {
                                serde_json::Value::String(s) => Some(s.as_str()),
                                _ => None,
                            },
                            match value {
                                serde_json::Value::Number(n) if n.is_i64() => n.as_i64(),
                                _ => None,
                            },
                            match value {
                                serde_json::Value::Number(n) => n.as_f64(),
                                _ => None,
                            },
                            match value {
                                serde_json::Value::Bool(b) => Some(*b as i32),
                                _ => None,
                            },
                        ],
                    )?;

                    // Get the attribute id
                    let attr_id: i64 = tx.query_row(
                        &format!(
                            r#"
                            SELECT id FROM {} 
                            WHERE key = ?1 
                            AND (
                                (value_type = 'string' AND string_value = ?2) OR
                                (value_type = 'int' AND int_value = ?3) OR
                                (value_type = 'double' AND double_value = ?4) OR
                                (value_type = 'bool' AND bool_value = ?5)
                            )
                            "#,
                            table_name
                        ),
                        params![
                            key,
                            match value {
                                serde_json::Value::String(s) => Some(s.as_str()),
                                _ => None,
                            },
                            match value {
                                serde_json::Value::Number(n) if n.is_i64() => n.as_i64(),
                                _ => None,
                            },
                            match value {
                                serde_json::Value::Number(n) => n.as_f64(),
                                _ => None,
                            },
                            match value {
                                serde_json::Value::Bool(b) => Some(*b as i32),
                                _ => None,
                            },
                        ],
                        |row| row.get(0),
                    )?;

                    // Link the log to the attribute
                    tx.execute(
                        &format!(
                            "INSERT INTO {} (log_id, attr_id) VALUES (?1, ?2)",
                            link_table
                        ),
                        params![log_id, attr_id],
                    )?;
                }
                Ok(())
            }

            // Store log attributes
            if let Some(obj) = log.attributes.as_object() {
                store_attributes(&tx, log_id, obj, "log_attributes", "logs_to_log_attrs")?;
            }

            // Store resource attributes
            if let Some(resource_attrs) = &log.resource_attributes {
                if let Some(obj) = resource_attrs.as_object() {
                    store_attributes(
                        &tx,
                        log_id,
                        obj,
                        "resource_attributes",
                        "logs_to_resource_attrs",
                    )?;
                }
            }

            tx.commit()?;
            Ok(())
        }

        pub fn store_trace(&mut self, trace: &DbTraceRecord) -> Result<()> {
            let tx = self.conn.transaction()?;

            // Insert main trace record
            tx.execute(
                r#"
                INSERT INTO traces (
                    trace_id, span_id, parent_span_id, name, kind,
                    start_time_unix_nano, end_time_unix_nano,
                    status_code, status_message, flags
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                "#,
                params![
                    trace.trace_id,
                    trace.span_id,
                    trace.parent_span_id,
                    trace.name,
                    trace.kind,
                    trace.start_time_unix_nano.to_string(),
                    trace.end_time_unix_nano.to_string(),
                    trace.status_code,
                    trace.status_message,
                    trace.flags,
                ],
            )?;

            let trace_id = tx.last_insert_rowid();

            // Helper function to store attributes in span tables
            fn store_span_attributes(
                tx: &Transaction,
                span_id: i64,
                attributes: &serde_json::Map<String, serde_json::Value>,
                is_resource: bool,
            ) -> Result<()> {
                let (table_name, link_table) = if is_resource {
                    ("span_resource_attributes", "spans_to_resource_attrs")
                } else {
                    ("span_attributes", "spans_to_span_attrs")
                };

                for (key, value) in attributes {
                    // First check if the attribute already exists
                    let attr_id: Option<i64> = tx.query_row(
                        &format!(
                            r#"
                            SELECT id FROM {} 
                            WHERE key = ?1 
                            AND value_type = ?2
                            AND (
                                (value_type = 'string' AND string_value = ?3) OR
                                (value_type = 'int' AND int_value = ?4) OR
                                (value_type = 'double' AND double_value = ?5) OR
                                (value_type = 'bool' AND bool_value = ?6)
                            )
                            "#,
                            table_name
                        ),
                        params![
                            key,
                            match value {
                                serde_json::Value::String(_) => "string",
                                serde_json::Value::Number(n) if n.is_i64() => "int",
                                serde_json::Value::Number(_) => "double",
                                serde_json::Value::Bool(_) => "bool",
                                _ => continue,
                            },
                            match value {
                                serde_json::Value::String(s) => Some(s.as_str()),
                                _ => None,
                            },
                            match value {
                                serde_json::Value::Number(n) if n.is_i64() => n.as_i64(),
                                _ => None,
                            },
                            match value {
                                serde_json::Value::Number(n) => n.as_f64(),
                                _ => None,
                            },
                            match value {
                                serde_json::Value::Bool(b) => Some(*b as i32),
                                _ => None,
                            },
                        ],
                        |row| row.get(0),
                    ).optional()?;

                    let attr_id = if let Some(id) = attr_id {
                        // Attribute already exists, use its ID
                        id
                    } else {
                        // Attribute doesn't exist, insert it
                        tx.execute(
                            &format!(
                                r#"
                                INSERT INTO {} (
                                    key, value_type, string_value, int_value, double_value, bool_value
                                )
                                VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                                "#,
                                table_name
                            ),
                            params![
                                key,
                                match value {
                                    serde_json::Value::String(_) => "string",
                                    serde_json::Value::Number(n) if n.is_i64() => "int",
                                    serde_json::Value::Number(_) => "double",
                                    serde_json::Value::Bool(_) => "bool",
                                    _ => continue,
                                },
                                match value {
                                    serde_json::Value::String(s) => Some(s.as_str()),
                                    _ => None,
                                },
                                match value {
                                    serde_json::Value::Number(n) if n.is_i64() => n.as_i64(),
                                    _ => None,
                                },
                                match value {
                                    serde_json::Value::Number(n) => n.as_f64(),
                                    _ => None,
                                },
                                match value {
                                    serde_json::Value::Bool(b) => Some(*b as i32),
                                    _ => None,
                                },
                            ],
                        )?;
                        tx.last_insert_rowid()
                    };

                    // Link the span to the attribute if not already linked
                    tx.execute(
                        &format!(
                            r#"
                            INSERT OR IGNORE INTO {} (span_id, attr_id) 
                            VALUES (?1, ?2)
                            "#,
                            link_table
                        ),
                        params![span_id, attr_id],
                    )?;
                }
                Ok(())
            }

            // Store span attributes
            if let Some(obj) = trace.attributes.as_object() {
                store_span_attributes(&tx, trace_id, obj, false)?;
            }

            // Store resource attributes
            if let Some(resource_attrs) = &trace.resource_attributes {
                if let Some(obj) = resource_attrs.as_object() {
                    store_span_attributes(&tx, trace_id, obj, true)?;
                }
            }

            tx.commit()?;
            Ok(())
        }

        // pub fn query_spans_by_trace_id(&self, trace_id: &str) -> Result<Vec<DbTraceRecord>> {
        //     let mut stmt = self.conn.prepare(
        //         r#"
        //         SELECT * FROM traces 
        //         WHERE trace_id = ?1 
        //         ORDER BY start_time_unix_nano ASC
        //         "#,
        //     )?;

        //     let spans = stmt
        //         .query_map([trace_id], |row| {
        //             Ok(DbTraceRecord {
        //                 trace_id: row.get("trace_id")?,
        //                 span_id: row.get("span_id")?,
        //                 parent_span_id: row.get("parent_span_id")?,
        //                 name: row.get("name")?,
        //                 kind: row.get("kind")?,
        //                 start_time_unix_nano: row
        //                     .get::<_, String>("start_time_unix_nano")?
        //                     .parse()
        //                     .unwrap_or_default(),
        //                 end_time_unix_nano: row
        //                     .get::<_, String>("end_time_unix_nano")?
        //                     .parse()
        //                     .unwrap_or_default(),
        //                 attributes: self
        //                     .get_span_attributes(row.get::<_, String>("span_id")?)
        //                     .unwrap_or_default(),
        //                 resource_attributes: Some(
        //                     self.get_span_resource_attributes(row.get::<_, String>("span_id")?)
        //                         .unwrap_or_default(),
        //                 ),
        //                 scope_info: None, // Add if needed
        //                 status_code: row.get("status_code")?,
        //                 status_message: row.get("status_message")?,
        //             })
        //         })?
        //         .collect::<Result<Vec<_>>>()?;

        //     Ok(spans)
        // }

        pub fn query_span_by_id(
            &self,
            trace_id: &str,
            span_id: &str,
        ) -> Result<Option<DbTraceRecord>> {
            let mut stmt = self.conn.prepare(
                r#"
                SELECT * FROM traces 
                WHERE trace_id = ?1 AND span_id = ?2
                "#,
            )?;

            let mut spans = stmt
                .query_map([trace_id, span_id], |row| {
                    Ok(DbTraceRecord {
                        id: row.get("id")?,
                        trace_id: row.get("trace_id")?,
                        span_id: row.get("span_id")?,
                        parent_span_id: row.get("parent_span_id")?,
                        name: row.get("name")?,
                        kind: row.get("kind")?,
                        start_time_unix_nano: row
                            .get::<_, String>("start_time_unix_nano")?
                            .parse()
                            .unwrap_or_default(),
                        end_time_unix_nano: row
                            .get::<_, String>("end_time_unix_nano")?
                            .parse()
                            .unwrap_or_default(),
                        attributes: self
                            .get_span_attributes(row.get("id")?)
                            .unwrap_or_default(),
                        resource_attributes: Some(
                            self.get_span_resource_attributes(row.get("id")?)
                                .unwrap_or_default(),
                        ),
                        scope_info: None, // Add if needed
                        status_code: row.get("status_code")?,
                        status_message: row.get("status_message")?,
                        flags: row.get("flags")?,
                    })
                })?
                .collect::<Result<Vec<_>>>()?;

            Ok(spans.pop())
        }

        fn get_span_attributes(&self, span_id: i64) -> Result<serde_json::Value> {
            let mut stmt = self.conn.prepare(
                r#"
                SELECT key, value_type, string_value, int_value, double_value, bool_value
                FROM span_attributes sa
                JOIN spans_to_span_attrs sta ON sa.id = sta.attr_id
                WHERE sta.span_id = ?1
                "#,
            )?;

            let mut map = serde_json::Map::new();
            let rows = stmt.query_map([span_id], |row| {
                let key: String = row.get("key")?;
                let value_type: String = row.get("value_type")?;
                let value = match value_type.as_str() {
                    "string" => serde_json::Value::String(row.get("string_value")?),
                    "int" => serde_json::Value::Number(row.get::<_, i64>("int_value")?.into()),
                    "double" => serde_json::Value::Number(
                        serde_json::Number::from_f64(row.get("double_value")?)
                            .unwrap_or(serde_json::Number::from(0)),
                    ),
                    "bool" => serde_json::Value::Bool(row.get::<_, i32>("bool_value")? != 0),
                    _ => serde_json::Value::Null,
                };
                Ok((key, value))
            })?;

            for row in rows {
                let (key, value) = row?;
                map.insert(key, value);
            }

            Ok(serde_json::Value::Object(map))
        }

        fn get_span_resource_attributes(&self, span_id: i64) -> Result<serde_json::Value> {
            let mut stmt = self.conn.prepare(
                r#"
                SELECT key, value_type, string_value, int_value, double_value, bool_value
                FROM span_resource_attributes sra
                JOIN spans_to_resource_attrs stra ON sra.id = stra.attr_id
                WHERE stra.span_id = ?1
                "#,
            )?;

            let mut map = serde_json::Map::new();
            let rows = stmt.query_map([span_id], |row| {
                let key: String = row.get("key")?;
                let value_type: String = row.get("value_type")?;
                let value = match value_type.as_str() {
                    "string" => serde_json::Value::String(row.get("string_value")?),
                    "int" => serde_json::Value::Number(row.get::<_, i64>("int_value")?.into()),
                    "double" => serde_json::Value::Number(
                        serde_json::Number::from_f64(row.get("double_value")?)
                            .unwrap_or(serde_json::Number::from(0)),
                    ),
                    "bool" => serde_json::Value::Bool(row.get::<_, i32>("bool_value")? != 0),
                    _ => serde_json::Value::Null,
                };
                Ok((key, value))
            })?;

            for row in rows {
                let (key, value) = row?;
                map.insert(key, value);
            }

            Ok(serde_json::Value::Object(map))
        }

        pub fn query_root_traces(
            &self,
            limit: Option<i64>,
            from_time: Option<u64>,
            to_time: Option<u64>,
        ) -> Result<Vec<DbTraceRecord>> {
            let mut query = String::from(
                r#"
                SELECT * FROM traces 
                WHERE parent_span_id IS NULL
                "#,
            );
            let mut params: Vec<Box<dyn ToSql>> = vec![];

            if let Some(from) = from_time {
                query.push_str(" AND start_time_unix_nano >= ?");
                params.push(Box::new(from.to_string()));
            }

            if let Some(to) = to_time {
                query.push_str(" AND start_time_unix_nano <= ?");
                params.push(Box::new(to.to_string()));
            }

            query.push_str(" ORDER BY start_time_unix_nano DESC");
            query.push_str(&format!(" LIMIT {}", limit.unwrap_or(100)));

            let mut stmt = self.conn.prepare(&query)?;

            let traces = stmt
                .query_map(params_from_iter(params.iter().map(|p| p.as_ref())), |row| {
                    Ok(DbTraceRecord {
                        id: row.get("id")?,
                        trace_id: row.get("trace_id")?,
                        span_id: row.get("span_id")?,
                        parent_span_id: row.get("parent_span_id")?,
                        name: row.get("name")?,
                        kind: row.get("kind")?,
                        start_time_unix_nano: row
                            .get::<_, String>("start_time_unix_nano")?
                            .parse()
                            .unwrap_or_default(),
                        end_time_unix_nano: row
                            .get::<_, String>("end_time_unix_nano")?
                            .parse()
                            .unwrap_or_default(),
                        attributes: self
                            .get_span_attributes(row.get("id")?)
                            .unwrap_or_default(),
                        resource_attributes: Some(
                            self.get_span_resource_attributes(row.get("id")?)
                                .unwrap_or_default(),
                        ),
                        scope_info: None,
                        status_code: row.get("status_code")?,
                        status_message: row.get("status_message")?,
                        flags: row.get("flags")?,
                    })
                })?
                .collect::<Result<Vec<_>>>()?;

            Ok(traces)
        }
    }
}

// Error handling
#[derive(Debug)]
enum AppError {
    Database(rusqlite::Error),
    Serialization(serde_json::Error),
}

impl From<AppError> for StatusCode {
    fn from(error: AppError) -> Self {
        match error {
            AppError::Database(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AppError::Serialization(_) => StatusCode::BAD_REQUEST,
        }
    }
}
