use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};
use uuid::Uuid;

static AUDIT_LOG_PATH: OnceLock<Mutex<Option<PathBuf>>> = OnceLock::new();

/// Initialize the audit log file path (called once during logger setup)
/// Validates that the path exists and is writable before storing it
/// Returns an error if the path is configured but invalid
pub fn init_audit_log_path(path: Option<String>) -> anyhow::Result<()> {
    let validated_path = if let Some(p) = path {
        let path_buf = PathBuf::from(&p);
        
        if !path_buf.exists() {
            anyhow::bail!("Audit log path does not exist: {}", p);
        }
        
        if !path_buf.is_dir() {
            anyhow::bail!("Audit log path is not a directory: {}", p);
        }
        
        // Test if we can write to the directory by attempting to create a test file
        let test_file_path = path_buf.join(".audit_write_test");
        match OpenOptions::new()
            .create(true)
            .write(true)
            .open(&test_file_path)
        {
            Ok(_) => {
                let _ = std::fs::remove_file(&test_file_path);
                Some(path_buf)
            }
            Err(e) => {
                anyhow::bail!("Audit log path is not writable: {}. Error: {}", p, e);
            }
        }
    } else {
        None
    };
    
    AUDIT_LOG_PATH.get_or_init(|| Mutex::new(validated_path));
    Ok(())
}

/// Write audit event to file if configured
fn write_to_audit_file(event: &AuditEvent) {
    if let Some(lock) = AUDIT_LOG_PATH.get() {
        if let Ok(guard) = lock.lock() {
            if let Some(base_path) = guard.as_ref() {
                // Get current date in YYYY-MM-DD format
                let date = chrono::Utc::now().format("%Y-%m-%d").to_string();
                
                let filename = format!("qdrant-audit_{}.log", date);
                
                let log_path = base_path.join(&filename);
                
                // Try to append to the file, create if it doesn't exist
                // If both opening and writing fail, log an error to standard tracing
                // We don't push the audit event to standard tracing since it may
                // be sensitive, and we don't force a crash
                // Perhaps it makes sense to warn on low volume capacity?
                // Not sure how we can do this safely, esp. for network volumes
                match OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&log_path)
                {
                    Ok(mut file) => {
                        // Serialize to JSON and write with newline
                        match serde_json::to_string(event) {
                            Ok(json) => {
                                if let Err(e) = writeln!(file, "{}", json) {
                                    error!(
                                        "Failed to write audit event to file {}: {}",
                                        log_path.display(),
                                        e
                                    );
                                }
                            }
                            Err(e) => {
                                error!("Failed to serialize audit event to JSON: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            "Failed to open audit log file {}: {}",
                            log_path.display(),
                            e
                        );
                    }
                }
            }
        }
    }
}

/// Authentication type for the audit event
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuthType {
    ApiKey,
    Jwt,
    Anonymous,
}

/// Status of the audit event
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Accepted,
    Success,
    Failure,
}

/// Audit event structure for tracking operations in Qdrant
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    /// Authentication type used for this operation
    #[serde(rename = "authType")]
    pub auth_type: AuthType,

    /// UTC timestamp of when the event occurred
    pub timestamp: String,

    /// Unique identifier for this event
    pub uuid: String,

    /// IP address of the client making the request
    pub remote: String,

    /// IP address of the local Qdrant server
    pub local: String,

    /// The action being performed (e.g., "delete_collection", "upsert_point")
    pub operation: String,

    /// Status of the operation
    pub status: Status,

    /// Subject claim from JWT token (empty string if not using JWT)
    pub subject: String,

    /// Name of the collection being operated on (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collection: Option<String>,

    /// Point ID being operated on (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub point: Option<String>,

    /// Result or error message for the operation (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,

    /// Operation timing in milliseconds (optional, defaults to -1 if not set)
    #[serde(default = "default_timing")]
    pub timing: i64,
}

fn default_timing() -> i64 {
    -1
}

impl AuditEvent {
    /// Create a new audit event with required fields
    pub fn new(
        auth_type: AuthType,
        remote: String,
        local: String,
        operation: String,
        status: Status,
    ) -> Self {
        Self {
            auth_type,
            timestamp: chrono::Utc::now().to_rfc3339(),
            uuid: Uuid::new_v4().to_string(),
            remote,
            local,
            operation,
            status,
            subject: String::new(),
            collection: None,
            point: None,
            result: None,
            timing: -1,
        }
    }

    /// Set the subject (from JWT)
    pub fn with_subject(mut self, subject: String) -> Self {
        self.subject = subject;
        self
    }

    /// Set the collection name
    pub fn with_collection(mut self, collection: String) -> Self {
        self.collection = Some(collection);
        self
    }

    /// Set the point ID
    pub fn with_point(mut self, point: String) -> Self {
        self.point = Some(point);
        self
    }

    /// Set the result message
    pub fn with_result(mut self, result: String) -> Self {
        self.result = Some(result);
        self
    }

    /// Set the status
    pub fn with_status(mut self, status: Status) -> Self {
        self.status = status;
        self
    }

    /// Set the operation timing in milliseconds
    pub fn with_timing(mut self, timing: i64) -> Self {
        self.timing = timing;
        self
    }

    /// Log this audit event as an info log with structured fields
    pub fn log_info(&self) {
        // Write to audit file if configured
        write_to_audit_file(self);
        
        // Also log to standard tracing output
        info!(
            authType = ?self.auth_type,
            timestamp = %self.timestamp,
            uuid = %self.uuid,
            remote = %self.remote,
            local = %self.local,
            operation = %self.operation,
            status = ?self.status,
            subject = %self.subject,
            collection = ?self.collection,
            point = ?self.point,
            result = ?self.result,
            timing = %self.timing,
            "Audit event"
        );
    }

    /// Log this audit event as an info log with a custom message
    pub fn log_info_with_message(&self, message: &str) {
        // Write to audit file if configured
        write_to_audit_file(self);
        
        // Also log to standard tracing output
        info!(
            authType = ?self.auth_type,
            timestamp = %self.timestamp,
            uuid = %self.uuid,
            remote = %self.remote,
            local = %self.local,
            operation = %self.operation,
            status = ?self.status,
            subject = %self.subject,
            collection = ?self.collection,
            point = ?self.point,
            result = ?self.result,
            timing = %self.timing,
            "{}", message
        );
    }
}