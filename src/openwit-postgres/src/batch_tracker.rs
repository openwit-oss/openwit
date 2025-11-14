use anyhow::{Result, Context};
use chrono::{DateTime, Utc};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use tracing::{info, error, warn, debug};

/// Batch tracking system for pipeline progress monitoring
pub struct BatchTracker {
    db_pool: PgPool,
}

/// Represents a batch in the tracking system
#[derive(Debug, Clone)]
pub struct BatchRecord {
    pub batch_id: String,
    pub source_type: String,
    pub client_id: String,
    pub data_pip: bool,
    pub wal_pip: bool,
    pub ingestion_pip: bool,
    pub storage_pip: bool,
    pub index_pip: bool,
    pub janitor_pip: bool,
    pub batch_size: i32,
    pub message_count: i32,
    pub first_offset: Option<i64>,
    pub last_offset: Option<i64>,
    pub topics: Vec<String>,
    pub peer_address: Option<String>,
    pub telemetry_type: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl BatchTracker {
    /// Create a new batch tracker with PostgreSQL connection
    pub async fn new(database_url: &str) -> Result<Self> {
        info!("Initializing batch tracker with PostgreSQL at: {}", database_url);

        // Parse and validate the connection URL
        if !database_url.contains("://") {
            return Err(anyhow::anyhow!("Invalid database URL format. Expected: postgresql://[user[:password]@][host][:port][/database]"));
        }

        // Try to connect with retries
        let mut retry_count = 0;
        let max_retries = 3;
        let db_pool = loop {
            match PgPoolOptions::new()
                .max_connections(10)
                .min_connections(1)
                .acquire_timeout(std::time::Duration::from_secs(30))
                .connect(database_url)
                .await
            {
                Ok(pool) => {
                    info!("Successfully connected to PostgreSQL for batch tracking");
                    break pool;
                }
                Err(e) => {
                    let error_msg = e.to_string();

                    // Check if database doesn't exist
                    if error_msg.contains("database") && error_msg.contains("does not exist") {
                        info!("Database does not exist, attempting to create it automatically...");

                        // Try to create the database
                        match Self::create_database_if_missing(database_url).await {
                            Ok(_) => {
                                info!("Database created successfully, retrying connection...");
                                // Retry connection immediately
                                continue;
                            }
                            Err(create_err) => {
                                error!("Failed to create database: {}", create_err);
                                error!("Please create the database manually: createdb openwit");
                                return Err(anyhow::anyhow!("Failed to create database: {}", create_err));
                            }
                        }
                    }

                    retry_count += 1;
                    if retry_count >= max_retries {
                        error!("Failed to connect to PostgreSQL after {} attempts: {}", max_retries, e);
                        error!("Please ensure PostgreSQL is running and accessible at: {}", database_url);
                        error!("Common issues:");
                        error!("  - PostgreSQL service not running");
                        error!("  - Authentication failed (check pg_hba.conf)");
                        error!("  - Connection refused (check postgresql.conf for listen_addresses)");
                        return Err(anyhow::anyhow!("Failed to create PostgreSQL connection pool: {}", e));
                    }
                    warn!("Failed to connect to PostgreSQL (attempt {}/{}): {}. Retrying in 5 seconds...",
                          retry_count, max_retries, e);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            }
        };

        let tracker = Self { db_pool };

        // Initialize schema
        tracker.initialize_schema().await?;

        Ok(tracker)
    }

    /// Create database if it doesn't exist
    async fn create_database_if_missing(database_url: &str) -> Result<()> {
        // Parse the database URL to extract database name
        let db_name = database_url
            .split('/')
            .last()
            .and_then(|s| s.split('?').next())
            .ok_or_else(|| anyhow::anyhow!("Failed to parse database name from URL"))?;

        // Create URL to postgres database (default database that always exists)
        let postgres_url = database_url.replace(&format!("/{}", db_name), "/postgres");

        info!("Connecting to default 'postgres' database to create '{}'", db_name);

        // Connect to default postgres database
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(std::time::Duration::from_secs(10))
            .connect(&postgres_url)
            .await
            .context("Failed to connect to default 'postgres' database")?;

        // Create the database
        let create_query = format!("CREATE DATABASE {}", db_name);
        sqlx::raw_sql(&create_query)
            .execute(&pool)
            .await
            .context(format!("Failed to execute CREATE DATABASE {}", db_name))?;

        info!("Successfully created database '{}'", db_name);
        Ok(())
    }
    
    /// Test database connectivity without failing
    pub async fn test_connection(database_url: &str) -> bool {
        match PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(std::time::Duration::from_secs(5))
            .connect(database_url)
            .await
        {
            Ok(pool) => {
                // Try a simple query
                match sqlx::query("SELECT 1").execute(&pool).await {
                    Ok(_) => {
                        debug!("PostgreSQL connection test successful");
                        true
                    }
                    Err(e) => {
                        debug!("PostgreSQL query test failed: {}", e);
                        false
                    }
                }
            }
            Err(e) => {
                debug!("PostgreSQL connection test failed: {}", e);
                false
            }
        }
    }
    
    /// Initialize the batch_tracker table if it doesn't exist
    async fn initialize_schema(&self) -> Result<()> {
        info!("Initializing batch_tracker schema");
        
        let query = r#"
        CREATE TABLE IF NOT EXISTS batch_tracker (
            batch_id VARCHAR(255) PRIMARY KEY,
            source_type VARCHAR(20) NOT NULL,
            client_id VARCHAR(255) NOT NULL,

            -- Pipeline stages (universal naming)
            data_pip BOOLEAN DEFAULT false,
            wal_pip BOOLEAN DEFAULT false,
            ingestion_pip BOOLEAN DEFAULT false,
            storage_pip BOOLEAN DEFAULT false,
            index_pip BOOLEAN DEFAULT false,
            janitor_pip BOOLEAN DEFAULT false,

            -- Batch metadata
            batch_size INTEGER NOT NULL,
            message_count INTEGER NOT NULL,

            -- Source-specific fields (Kafka)
            first_offset BIGINT,
            last_offset BIGINT,
            topics TEXT[],

            -- Source-specific fields (gRPC)
            peer_address VARCHAR(255),
            telemetry_type VARCHAR(20),

            -- Timing information
            created_at TIMESTAMPTZ DEFAULT NOW(),
            data_completed_at TIMESTAMPTZ,
            wal_completed_at TIMESTAMPTZ,
            ingestion_completed_at TIMESTAMPTZ,
            storage_completed_at TIMESTAMPTZ,
            index_completed_at TIMESTAMPTZ,
            janitor_completed_at TIMESTAMPTZ,

            -- Error tracking
            error_stage VARCHAR(50),
            error_message TEXT,
            retry_count INTEGER DEFAULT 0,

            -- Performance metrics
            data_duration_ms INTEGER,
            wal_duration_ms INTEGER,
            ingestion_duration_ms INTEGER
        );

        -- Create indexes
        CREATE INDEX IF NOT EXISTS idx_batch_tracker_source_type ON batch_tracker(source_type);
        CREATE INDEX IF NOT EXISTS idx_batch_tracker_client_id ON batch_tracker(client_id);
        CREATE INDEX IF NOT EXISTS idx_batch_tracker_created_at ON batch_tracker(created_at);
        CREATE INDEX IF NOT EXISTS idx_batch_tracker_status ON batch_tracker(data_pip, wal_pip, ingestion_pip);
        "#;
        
        sqlx::raw_sql(query)
            .execute(&self.db_pool)
            .await
            .context("Failed to create batch_tracker table")?;
        
        info!("Batch tracker schema initialized successfully");
        Ok(())
    }
    
    /// Create a new batch record (universal for Kafka and gRPC)
    pub async fn create_batch(
        &self,
        batch_id: String,
        source_type: String,
        client_id: String,
        batch_size: i32,
        message_count: i32,
        first_offset: Option<i64>,
        last_offset: Option<i64>,
        topics: Vec<String>,
        peer_address: Option<String>,
        telemetry_type: Option<String>,
    ) -> Result<String> {
        debug!(
            "Creating batch record: id={}, source={}, client_id={}, size={}, messages={}",
            batch_id, source_type, client_id, batch_size, message_count
        );

        let query = r#"
        INSERT INTO batch_tracker (
            batch_id, source_type, client_id, data_pip, data_completed_at,
            batch_size, message_count,
            first_offset, last_offset, topics,
            peer_address, telemetry_type
        ) VALUES ($1, $2, $3, true, NOW(), $4, $5, $6, $7, $8, $9, $10)
        "#;

        sqlx::query(query)
            .bind(&batch_id)
            .bind(&source_type)
            .bind(&client_id)
            .bind(batch_size)
            .bind(message_count)
            .bind(first_offset)
            .bind(last_offset)
            .bind(&topics)
            .bind(&peer_address)
            .bind(&telemetry_type)
            .execute(&self.db_pool)
            .await
            .context("Failed to create batch record")?;

        info!("Created batch {} ({}) for client {}", batch_id, source_type, client_id);
        Ok(batch_id)
    }
    
    /// Update WAL pipeline status
    pub async fn update_wal_completed(&self, batch_id: &str) -> Result<()> {
        debug!("Updating WAL completed for batch {}", batch_id);

        let query = r#"
        UPDATE batch_tracker
        SET wal_pip = true,
            wal_completed_at = NOW(),
            data_duration_ms = EXTRACT(EPOCH FROM (NOW() - data_completed_at)) * 1000
        WHERE batch_id = $1
        "#;

        let rows_affected = sqlx::query(query)
            .bind(batch_id)
            .execute(&self.db_pool)
            .await
            .context("Failed to update WAL status")?
            .rows_affected();

        if rows_affected == 0 {
            warn!("No batch found with id {}", batch_id);
        } else {
            debug!("WAL completed for batch {}", batch_id);
        }

        Ok(())
    }
    
    /// Update data pipeline status (deprecated - data_pip set on creation)
    #[allow(dead_code)]
    pub async fn update_data_completed(&self, batch_id: &str) -> Result<()> {
        debug!("Updating data completed for batch {}", batch_id);

        let query = r#"
        UPDATE batch_tracker
        SET data_pip = true,
            data_completed_at = NOW()
        WHERE batch_id = $1
        "#;

        let rows_affected = sqlx::query(query)
            .bind(batch_id)
            .execute(&self.db_pool)
            .await
            .context("Failed to update data status")?
            .rows_affected();

        if rows_affected == 0 {
            warn!("No batch found with id {}", batch_id);
        } else {
            debug!("Data completed for batch {}", batch_id);
        }

        Ok(())
    }

    /// Update ingestion pipeline status
    pub async fn update_ingestion_completed(&self, batch_id: &str) -> Result<()> {
        debug!("Updating ingestion completed for batch {}", batch_id);
        
        let query = r#"
        UPDATE batch_tracker 
        SET ingestion_pip = true, 
            ingestion_completed_at = NOW(),
            wal_duration_ms = CASE 
                WHEN wal_completed_at IS NOT NULL 
                THEN EXTRACT(EPOCH FROM (NOW() - wal_completed_at)) * 1000
                ELSE NULL 
            END
        WHERE batch_id = $1
        "#;
        
        sqlx::query(query)
            .bind(batch_id)
            .execute(&self.db_pool)
            .await
            .context("Failed to update ingestion status")?;
        
        Ok(())
    }
    
    /// Update storage pipeline status
    #[allow(dead_code)]
    pub async fn update_storage_completed(&self, batch_id: &str) -> Result<()> {
        debug!("Updating storage completed for batch {}", batch_id);

        let query = r#"
        UPDATE batch_tracker
        SET storage_pip = true,
            storage_completed_at = NOW()
        WHERE batch_id = $1
        "#;

        sqlx::query(query)
            .bind(batch_id)
            .execute(&self.db_pool)
            .await
            .context("Failed to update storage status")?;

        Ok(())
    }

    /// Update index pipeline status
    #[allow(dead_code)]
    pub async fn update_index_completed(&self, batch_id: &str) -> Result<()> {
        debug!("Updating index completed for batch {}", batch_id);

        let query = r#"
        UPDATE batch_tracker
        SET index_pip = true,
            index_completed_at = NOW()
        WHERE batch_id = $1
        "#;

        sqlx::query(query)
            .bind(batch_id)
            .execute(&self.db_pool)
            .await
            .context("Failed to update index status")?;

        Ok(())
    }

    /// Update janitor pipeline status
    #[allow(dead_code)]
    pub async fn update_janitor_completed(&self, batch_id: &str) -> Result<()> {
        debug!("Updating janitor completed for batch {}", batch_id);

        let query = r#"
        UPDATE batch_tracker
        SET janitor_pip = true,
            janitor_completed_at = NOW()
        WHERE batch_id = $1
        "#;

        sqlx::query(query)
            .bind(batch_id)
            .execute(&self.db_pool)
            .await
            .context("Failed to update janitor status")?;

        Ok(())
    }

    /// Record an error for a batch at a specific stage
    #[allow(dead_code)]
    pub async fn record_error(&self, batch_id: &str, stage: &str, error_message: &str) -> Result<()> {
        error!("Recording error for batch {} at stage {}: {}", batch_id, stage, error_message);

        let query = r#"
        UPDATE batch_tracker
        SET error_stage = $2,
            error_message = $3,
            retry_count = retry_count + 1
        WHERE batch_id = $1
        "#;

        sqlx::query(query)
            .bind(batch_id)
            .bind(stage)
            .bind(error_message)
            .execute(&self.db_pool)
            .await
            .context("Failed to record error")?;

        Ok(())
    }

    /// Get batch status
    #[allow(dead_code)]
    pub async fn get_batch_status(&self, batch_id: &str) -> Result<Option<BatchRecord>> {
        let query = r#"
        SELECT batch_id, source_type, client_id, data_pip, wal_pip, ingestion_pip,
               storage_pip, index_pip, janitor_pip, batch_size, message_count,
               first_offset, last_offset, topics, peer_address, telemetry_type, created_at
        FROM batch_tracker
        WHERE batch_id = $1
        "#;

        let row = sqlx::query(query)
            .bind(batch_id)
            .fetch_optional(&self.db_pool)
            .await
            .context("Failed to fetch batch status")?;

        if let Some(row) = row {
            Ok(Some(BatchRecord {
                batch_id: row.get("batch_id"),
                source_type: row.get("source_type"),
                client_id: row.get("client_id"),
                data_pip: row.get("data_pip"),
                wal_pip: row.get("wal_pip"),
                ingestion_pip: row.get("ingestion_pip"),
                storage_pip: row.get("storage_pip"),
                index_pip: row.get("index_pip"),
                janitor_pip: row.get("janitor_pip"),
                batch_size: row.get("batch_size"),
                message_count: row.get("message_count"),
                first_offset: row.get("first_offset"),
                last_offset: row.get("last_offset"),
                topics: row.get("topics"),
                peer_address: row.get("peer_address"),
                telemetry_type: row.get("telemetry_type"),
                created_at: row.get("created_at"),
            }))
        } else {
            Ok(None)
        }
    }

    /// Get incomplete batches older than specified duration
    #[allow(dead_code)]
    pub async fn get_stuck_batches(&self, older_than_minutes: i32) -> Result<Vec<String>> {
        let query = r#"
        SELECT batch_id
        FROM batch_tracker
        WHERE NOT janitor_pip
        AND created_at < NOW() - INTERVAL '$1 minutes'
        "#;

        let rows = sqlx::query(query)
            .bind(older_than_minutes)
            .fetch_all(&self.db_pool)
            .await
            .context("Failed to fetch stuck batches")?;

        let batch_ids: Vec<String> = rows.iter()
            .map(|row| row.get("batch_id"))
            .collect();

        if !batch_ids.is_empty() {
            warn!("Found {} stuck batches older than {} minutes", batch_ids.len(), older_than_minutes);
        }

        Ok(batch_ids)
    }
    
    /// Get client throughput statistics
    pub async fn get_client_stats(&self, hours: i32) -> Result<Vec<(String, i64, i64)>> {
        let query = r#"
        SELECT client_id, 
               COUNT(*) as batch_count,
               SUM(message_count) as total_messages
        FROM batch_tracker
        WHERE created_at > NOW() - INTERVAL '$1 hours'
        GROUP BY client_id
        ORDER BY total_messages DESC
        "#;
        
        let rows = sqlx::query(query)
            .bind(hours)
            .fetch_all(&self.db_pool)
            .await
            .context("Failed to fetch client stats")?;
        
        let stats: Vec<(String, i64, i64)> = rows.iter()
            .map(|row| {
                (
                    row.get::<String, _>("client_id"),
                    row.get::<i64, _>("batch_count"),
                    row.get::<i64, _>("total_messages"),
                )
            })
            .collect();
        
        Ok(stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    #[ignore] // Requires PostgreSQL
    async fn test_batch_tracker() {
        let tracker = BatchTracker::new("postgresql://localhost/openwit_test")
            .await
            .unwrap();
        
        // Create a test batch
        let batch_id = tracker.create_batch(
            "test_client".to_string(),
            1024,
            10,
            Some(100),
            Some(109),
            vec!["test_topic".to_string()],
        ).await.unwrap();
        
        // Update stages
        tracker.update_wal_completed(batch_id).await.unwrap();
        tracker.update_ingestion_completed(batch_id).await.unwrap();
        
        // Check status
        let status = tracker.get_batch_status(batch_id).await.unwrap();
        assert!(status.is_some());
        
        let batch = status.unwrap();
        assert_eq!(batch.client_id, "test_client");
        assert!(batch.kafka_pip);
        assert!(batch.wal_pip);
        assert!(batch.ingestion_pip);
    }
}