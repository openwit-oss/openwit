//! PostgreSQL-based metastore implementation for centralized metadata management

use async_trait::async_trait;
use anyhow::{Result, Context};
use sqlx::{PgPool, postgres::PgPoolOptions, Row};
use tracing::{info, warn};

use crate::{
    MetaStore, PartitionMetadata, SegmentMetadata, SegmentStatus, TableMetadata, 
    TimeRange, FieldSchema, StorageStats, LifecycleStats,
};

/// PostgreSQL metastore configuration
#[derive(Debug, Clone)]
pub struct PostgresMetastoreConfig {
    pub connection_string: String,
    pub max_connections: u32,
    pub schema_name: String,
}

impl Default for PostgresMetastoreConfig {
    fn default() -> Self {
        Self {
            connection_string: "postgresql://postgres:password@localhost/openwit".to_string(),
            max_connections: 10,
            schema_name: "metastore".to_string(),
        }
    }
}

/// PostgreSQL-based metastore implementation
pub struct PostgresMetaStore {
    pool: PgPool,
    schema: String,
}

impl PostgresMetaStore {
    /// Create a new PostgreSQL metastore
    pub async fn new(config: PostgresMetastoreConfig) -> Result<Self> {
        info!("Connecting to PostgreSQL metastore: {}", config.connection_string);
        
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .connect(&config.connection_string)
            .await
            .context("Failed to connect to PostgreSQL")?;
        
        let metastore = Self {
            pool,
            schema: config.schema_name,
        };
        
        // Initialize schema
        metastore.init_schema().await?;
        
        info!("PostgreSQL metastore initialized successfully");
        Ok(metastore)
    }
    
    /// Initialize database schema
    async fn init_schema(&self) -> Result<()> {
        let schema = &self.schema;
        
        // Create schema if not exists
        sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {}", schema))
            .execute(&self.pool)
            .await?;
        
        // Create segment status enum
        sqlx::query(&format!(r#"
            DO $$ BEGIN
                CREATE TYPE {}.segment_status AS ENUM ('pending', 'stored', 'failed');
            EXCEPTION
                WHEN duplicate_object THEN null;
            END $$;
        "#, schema))
            .execute(&self.pool)
            .await?;
        
        // Create tables table
        sqlx::query(&format!(r#"
            CREATE TABLE IF NOT EXISTS {}.tables (
                table_name VARCHAR(255) PRIMARY KEY,
                schema JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        "#, schema))
            .execute(&self.pool)
            .await?;
        
        // Create partitions table
        sqlx::query(&format!(r#"
            CREATE TABLE IF NOT EXISTS {}.partitions (
                partition_id VARCHAR(255) PRIMARY KEY,
                time_start TIMESTAMPTZ NOT NULL,
                time_end TIMESTAMPTZ NOT NULL,
                segment_count INTEGER NOT NULL DEFAULT 0,
                row_count BIGINT NOT NULL DEFAULT 0,
                compressed_bytes BIGINT NOT NULL DEFAULT 0,
                uncompressed_bytes BIGINT NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        "#, schema))
            .execute(&self.pool)
            .await?;
        
        // Create segments table with new status fields
        sqlx::query(&format!(r#"
            CREATE TABLE IF NOT EXISTS {}.segments (
                segment_id VARCHAR(255) PRIMARY KEY,
                partition_id VARCHAR(255) NOT NULL,
                file_path TEXT NOT NULL,
                index_path TEXT,
                row_count BIGINT NOT NULL,
                compressed_bytes BIGINT NOT NULL,
                uncompressed_bytes BIGINT NOT NULL,
                min_timestamp TIMESTAMPTZ NOT NULL,
                max_timestamp TIMESTAMPTZ NOT NULL,
                storage_status {}.segment_status NOT NULL DEFAULT 'pending',
                index_status {}.segment_status NOT NULL DEFAULT 'pending',
                schema_version INTEGER NOT NULL DEFAULT 1,
                is_cold BOOLEAN NOT NULL DEFAULT FALSE,
                is_archived BOOLEAN NOT NULL DEFAULT FALSE,
                storage_class VARCHAR(50),
                transitioned_at TIMESTAMPTZ,
                archived_at TIMESTAMPTZ,
                restore_status VARCHAR(50),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                FOREIGN KEY (partition_id) REFERENCES {}.partitions(partition_id) ON DELETE CASCADE
            )
        "#, schema, schema, schema, schema))
            .execute(&self.pool)
            .await?;
        
        // Create errors table for tracking failures
        sqlx::query(&format!(r#"
            CREATE TABLE IF NOT EXISTS {}.segment_errors (
                id SERIAL PRIMARY KEY,
                segment_id VARCHAR(255) NOT NULL,
                error_type VARCHAR(50) NOT NULL, -- 'storage' or 'index'
                error_message TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                FOREIGN KEY (segment_id) REFERENCES {}.segments(segment_id) ON DELETE CASCADE
            )
        "#, schema, schema))
            .execute(&self.pool)
            .await?;
        
        // Create metadata table for WAL tracking
        sqlx::query(&format!(r#"
            CREATE TABLE IF NOT EXISTS {}.metadata (
                key VARCHAR(255) PRIMARY KEY,
                value BIGINT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        "#, schema))
            .execute(&self.pool)
            .await?;
        
        // Create indexes
        sqlx::query(&format!(r#"
            CREATE INDEX IF NOT EXISTS idx_partitions_time_range 
            ON {}.partitions (time_start, time_end)
        "#, schema))
            .execute(&self.pool)
            .await?;
        
        sqlx::query(&format!(r#"
            CREATE INDEX IF NOT EXISTS idx_segments_partition 
            ON {}.segments (partition_id)
        "#, schema))
            .execute(&self.pool)
            .await?;
        
        sqlx::query(&format!(r#"
            CREATE INDEX IF NOT EXISTS idx_segments_time_range 
            ON {}.segments (min_timestamp, max_timestamp)
        "#, schema))
            .execute(&self.pool)
            .await?;
        
        sqlx::query(&format!(r#"
            CREATE INDEX IF NOT EXISTS idx_segments_status 
            ON {}.segments (storage_status, index_status)
        "#, schema))
            .execute(&self.pool)
            .await?;
        
        // Create update trigger for updated_at
        sqlx::query(&format!(r#"
            CREATE OR REPLACE FUNCTION {}.update_updated_at()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = NOW();
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        "#, schema))
            .execute(&self.pool)
            .await?;
        
        // Apply trigger to tables
        for table in &["tables", "partitions", "segments"] {
            sqlx::query(&format!(r#"
                DROP TRIGGER IF EXISTS update_{}_updated_at ON {}.{};
                CREATE TRIGGER update_{}_updated_at
                BEFORE UPDATE ON {}.{}
                FOR EACH ROW
                EXECUTE FUNCTION {}.update_updated_at();
            "#, table, schema, table, table, schema, table, schema))
                .execute(&self.pool)
                .await?;
        }
        
        Ok(())
    }
    
    fn status_to_string(status: SegmentStatus) -> &'static str {
        match status {
            SegmentStatus::Pending => "pending",
            SegmentStatus::Stored => "stored",
            SegmentStatus::Failed => "failed",
        }
    }
    
    fn string_to_status(s: &str) -> SegmentStatus {
        match s {
            "stored" => SegmentStatus::Stored,
            "failed" => SegmentStatus::Failed,
            _ => SegmentStatus::Pending,
        }
    }
}

#[async_trait]
impl MetaStore for PostgresMetaStore {
    async fn create_table(&self, table: TableMetadata) -> Result<()> {
        let schema_json = serde_json::to_value(&table.schema)?;
        
        sqlx::query(&format!(r#"
            INSERT INTO {}.tables (table_name, schema, created_at, updated_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (table_name) DO UPDATE SET
                schema = EXCLUDED.schema,
                updated_at = EXCLUDED.updated_at
        "#, self.schema))
            .bind(&table.table_name)
            .bind(&schema_json)
            .bind(&table.created_at)
            .bind(&table.updated_at)
            .execute(&self.pool)
            .await?;
        
        Ok(())
    }
    
    async fn get_table(&self, table_name: &str) -> Result<Option<TableMetadata>> {
        let row = sqlx::query(&format!(r#"
            SELECT table_name, schema, created_at, updated_at
            FROM {}.tables
            WHERE table_name = $1
        "#, self.schema))
            .bind(table_name)
            .fetch_optional(&self.pool)
            .await?;
        
        match row {
            Some(row) => {
                let schema: serde_json::Value = row.try_get("schema")?;
                let schema: Vec<FieldSchema> = serde_json::from_value(schema)?;
                
                Ok(Some(TableMetadata {
                    table_name: row.try_get("table_name")?,
                    schema,
                    created_at: row.try_get("created_at")?,
                    updated_at: row.try_get("updated_at")?,
                }))
            }
            None => Ok(None),
        }
    }
    
    async fn create_partition(&self, metadata: PartitionMetadata) -> Result<()> {
        sqlx::query(&format!(r#"
            INSERT INTO {}.partitions (
                partition_id, time_start, time_end, segment_count,
                row_count, compressed_bytes, uncompressed_bytes,
                created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (partition_id) DO UPDATE SET
                segment_count = EXCLUDED.segment_count,
                row_count = EXCLUDED.row_count,
                compressed_bytes = EXCLUDED.compressed_bytes,
                uncompressed_bytes = EXCLUDED.uncompressed_bytes,
                updated_at = EXCLUDED.updated_at
        "#, self.schema))
            .bind(&metadata.partition_id)
            .bind(&metadata.time_range.start)
            .bind(&metadata.time_range.end)
            .bind(metadata.segment_count as i32)
            .bind(metadata.row_count as i64)
            .bind(metadata.compressed_bytes as i64)
            .bind(metadata.uncompressed_bytes as i64)
            .bind(&metadata.created_at)
            .bind(&metadata.updated_at)
            .execute(&self.pool)
            .await?;
        
        Ok(())
    }
    
    async fn get_partition(&self, partition_id: &str) -> Result<Option<PartitionMetadata>> {
        let row = sqlx::query(&format!(r#"
            SELECT partition_id, time_start, time_end, segment_count,
                   row_count, compressed_bytes, uncompressed_bytes,
                   created_at, updated_at
            FROM {}.partitions
            WHERE partition_id = $1
        "#, self.schema))
            .bind(partition_id)
            .fetch_optional(&self.pool)
            .await?;
        
        match row {
            Some(row) => Ok(Some(PartitionMetadata {
                partition_id: row.try_get("partition_id")?,
                time_range: TimeRange {
                    start: row.try_get("time_start")?,
                    end: row.try_get("time_end")?,
                },
                segment_count: row.try_get::<i32, _>("segment_count")? as usize,
                row_count: row.try_get::<i64, _>("row_count")? as u64,
                compressed_bytes: row.try_get::<i64, _>("compressed_bytes")? as u64,
                uncompressed_bytes: row.try_get::<i64, _>("uncompressed_bytes")? as u64,
                created_at: row.try_get("created_at")?,
                updated_at: row.try_get("updated_at")?,
            })),
            None => Ok(None),
        }
    }
    
    async fn list_partitions(&self, time_range: TimeRange) -> Result<Vec<PartitionMetadata>> {
        let rows = sqlx::query(&format!(r#"
            SELECT partition_id, time_start, time_end, segment_count,
                   row_count, compressed_bytes, uncompressed_bytes,
                   created_at, updated_at
            FROM {}.partitions
            WHERE time_start <= $2 AND time_end >= $1
            ORDER BY time_start
        "#, self.schema))
            .bind(&time_range.start)
            .bind(&time_range.end)
            .fetch_all(&self.pool)
            .await?;
        
        let partitions: Result<Vec<_>> = rows.into_iter().map(|row| {
            Ok(PartitionMetadata {
                partition_id: row.try_get("partition_id")?,
                time_range: TimeRange {
                    start: row.try_get("time_start")?,
                    end: row.try_get("time_end")?,
                },
                segment_count: row.try_get::<i32, _>("segment_count")? as usize,
                row_count: row.try_get::<i64, _>("row_count")? as u64,
                compressed_bytes: row.try_get::<i64, _>("compressed_bytes")? as u64,
                uncompressed_bytes: row.try_get::<i64, _>("uncompressed_bytes")? as u64,
                created_at: row.try_get("created_at")?,
                updated_at: row.try_get("updated_at")?,
            })
        }).collect();
        
        Ok(partitions?)
    }
    
    async fn create_segment(&self, metadata: SegmentMetadata) -> Result<()> {
        // Start transaction
        let mut tx = self.pool.begin().await?;
        
        // Insert segment
        sqlx::query(&format!(r#"
            INSERT INTO {}.segments (
                segment_id, partition_id, file_path, index_path,
                row_count, compressed_bytes, uncompressed_bytes,
                min_timestamp, max_timestamp, storage_status, index_status,
                schema_version, is_cold, is_archived, storage_class,
                transitioned_at, archived_at, restore_status,
                created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::{}::segment_status, 
                    $11::{}::segment_status, $12, $13, $14, $15, $16, $17, $18, $19, $20)
        "#, self.schema, self.schema, self.schema))
            .bind(&metadata.segment_id)
            .bind(&metadata.partition_id)
            .bind(&metadata.file_path)
            .bind(&metadata.index_path)
            .bind(metadata.row_count as i64)
            .bind(metadata.compressed_bytes as i64)
            .bind(metadata.uncompressed_bytes as i64)
            .bind(&metadata.min_timestamp)
            .bind(&metadata.max_timestamp)
            .bind(Self::status_to_string(metadata.storage_status))
            .bind(Self::status_to_string(metadata.index_status))
            .bind(metadata.schema_version as i32)
            .bind(metadata.is_cold)
            .bind(metadata.is_archived)
            .bind(&metadata.storage_class)
            .bind(&metadata.transitioned_at)
            .bind(&metadata.archived_at)
            .bind(&metadata.restore_status)
            .bind(&metadata.created_at)
            .bind(&metadata.updated_at)
            .execute(&mut *tx)
            .await?;
        
        // Update partition stats
        sqlx::query(&format!(r#"
            UPDATE {}.partitions SET
                segment_count = segment_count + 1,
                row_count = row_count + $2,
                compressed_bytes = compressed_bytes + $3,
                uncompressed_bytes = uncompressed_bytes + $4
            WHERE partition_id = $1
        "#, self.schema))
            .bind(&metadata.partition_id)
            .bind(metadata.row_count as i64)
            .bind(metadata.compressed_bytes as i64)
            .bind(metadata.uncompressed_bytes as i64)
            .execute(&mut *tx)
            .await?;
        
        tx.commit().await?;
        Ok(())
    }
    
    async fn update_segment(&self, segment_id: &str, metadata: SegmentMetadata) -> Result<()> {
        sqlx::query(&format!(r#"
            UPDATE {}.segments SET
                file_path = $2,
                index_path = $3,
                row_count = $4,
                compressed_bytes = $5,
                uncompressed_bytes = $6,
                min_timestamp = $7,
                max_timestamp = $8,
                storage_status = $9::{}::segment_status,
                index_status = $10::{}::segment_status,
                is_cold = $11,
                is_archived = $12,
                storage_class = $13,
                transitioned_at = $14,
                archived_at = $15,
                restore_status = $16
            WHERE segment_id = $1
        "#, self.schema, self.schema, self.schema))
            .bind(&segment_id)
            .bind(&metadata.file_path)
            .bind(&metadata.index_path)
            .bind(metadata.row_count as i64)
            .bind(metadata.compressed_bytes as i64)
            .bind(metadata.uncompressed_bytes as i64)
            .bind(&metadata.min_timestamp)
            .bind(&metadata.max_timestamp)
            .bind(Self::status_to_string(metadata.storage_status))
            .bind(Self::status_to_string(metadata.index_status))
            .bind(metadata.is_cold)
            .bind(metadata.is_archived)
            .bind(&metadata.storage_class)
            .bind(&metadata.transitioned_at)
            .bind(&metadata.archived_at)
            .bind(&metadata.restore_status)
            .execute(&self.pool)
            .await?;
        
        Ok(())
    }
    
    async fn get_segment(&self, segment_id: &str) -> Result<Option<SegmentMetadata>> {
        let row = sqlx::query(&format!(r#"
            SELECT segment_id, partition_id, file_path, index_path,
                   row_count, compressed_bytes, uncompressed_bytes,
                   min_timestamp, max_timestamp, storage_status, index_status,
                   schema_version, is_cold, is_archived, storage_class,
                   transitioned_at, archived_at, restore_status,
                   created_at, updated_at
            FROM {}.segments
            WHERE segment_id = $1
        "#, self.schema))
            .bind(segment_id)
            .fetch_optional(&self.pool)
            .await?;
        
        match row {
            Some(row) => Ok(Some(SegmentMetadata {
                segment_id: row.try_get("segment_id")?,
                partition_id: row.try_get("partition_id")?,
                file_path: row.try_get("file_path")?,
                index_path: row.try_get("index_path")?,
                row_count: row.try_get::<i64, _>("row_count")? as u64,
                compressed_bytes: row.try_get::<i64, _>("compressed_bytes")? as u64,
                uncompressed_bytes: row.try_get::<i64, _>("uncompressed_bytes")? as u64,
                min_timestamp: row.try_get("min_timestamp")?,
                max_timestamp: row.try_get("max_timestamp")?,
                storage_status: Self::string_to_status(row.try_get("storage_status")?),
                index_status: Self::string_to_status(row.try_get("index_status")?),
                schema_version: row.try_get::<i32, _>("schema_version")? as u32,
                is_cold: row.try_get("is_cold")?,
                is_archived: row.try_get("is_archived")?,
                storage_class: row.try_get("storage_class")?,
                transitioned_at: row.try_get("transitioned_at")?,
                archived_at: row.try_get("archived_at")?,
                restore_status: row.try_get("restore_status")?,
                created_at: row.try_get("created_at")?,
                updated_at: row.try_get("updated_at")?,
            })),
            None => Ok(None),
        }
    }
    
    async fn list_segments(&self, partition_id: &str) -> Result<Vec<SegmentMetadata>> {
        let rows = sqlx::query(&format!(r#"
            SELECT segment_id, partition_id, file_path, index_path,
                   row_count, compressed_bytes, uncompressed_bytes,
                   min_timestamp, max_timestamp, storage_status, index_status,
                   schema_version, is_cold, is_archived, storage_class,
                   transitioned_at, archived_at, restore_status,
                   created_at, updated_at
            FROM {}.segments
            WHERE partition_id = $1
            ORDER BY min_timestamp
        "#, self.schema))
            .bind(partition_id)
            .fetch_all(&self.pool)
            .await?;
        
        let segments: Result<Vec<_>> = rows.into_iter().map(|row| {
            Ok(SegmentMetadata {
                segment_id: row.try_get("segment_id")?,
                partition_id: row.try_get("partition_id")?,
                file_path: row.try_get("file_path")?,
                index_path: row.try_get("index_path")?,
                row_count: row.try_get::<i64, _>("row_count")? as u64,
                compressed_bytes: row.try_get::<i64, _>("compressed_bytes")? as u64,
                uncompressed_bytes: row.try_get::<i64, _>("uncompressed_bytes")? as u64,
                min_timestamp: row.try_get("min_timestamp")?,
                max_timestamp: row.try_get("max_timestamp")?,
                storage_status: Self::string_to_status(row.try_get("storage_status")?),
                index_status: Self::string_to_status(row.try_get("index_status")?),
                schema_version: row.try_get::<i32, _>("schema_version")? as u32,
                is_cold: row.try_get("is_cold")?,
                is_archived: row.try_get("is_archived")?,
                storage_class: row.try_get("storage_class")?,
                transitioned_at: row.try_get("transitioned_at")?,
                archived_at: row.try_get("archived_at")?,
                restore_status: row.try_get("restore_status")?,
                created_at: row.try_get("created_at")?,
                updated_at: row.try_get("updated_at")?,
            })
        }).collect();
        
        Ok(segments?)
    }
    
    async fn delete_segment(&self, segment_id: &str) -> Result<()> {
        // Start transaction
        let mut tx = self.pool.begin().await?;
        
        // Get segment info first
        let segment = self.get_segment(segment_id).await?;
        
        if let Some(segment) = segment {
            // Delete segment
            sqlx::query(&format!(r#"
                DELETE FROM {}.segments WHERE segment_id = $1
            "#, self.schema))
                .bind(segment_id)
                .execute(&mut *tx)
                .await?;
            
            // Update partition stats
            sqlx::query(&format!(r#"
                UPDATE {}.partitions SET
                    segment_count = GREATEST(0, segment_count - 1),
                    row_count = GREATEST(0, row_count - $2),
                    compressed_bytes = GREATEST(0, compressed_bytes - $3),
                    uncompressed_bytes = GREATEST(0, uncompressed_bytes - $4)
                WHERE partition_id = $1
            "#, self.schema))
                .bind(&segment.partition_id)
                .bind(segment.row_count as i64)
                .bind(segment.compressed_bytes as i64)
                .bind(segment.uncompressed_bytes as i64)
                .execute(&mut *tx)
                .await?;
        }
        
        tx.commit().await?;
        Ok(())
    }
    
    async fn delete_partition(&self, partition_id: &str) -> Result<()> {
        // Segments will be deleted automatically due to CASCADE
        sqlx::query(&format!(r#"
            DELETE FROM {}.partitions WHERE partition_id = $1
        "#, self.schema))
            .bind(partition_id)
            .execute(&self.pool)
            .await?;
        
        Ok(())
    }
    
    async fn get_storage_stats(&self) -> Result<StorageStats> {
        let row = sqlx::query(&format!(r#"
            SELECT 
                COUNT(DISTINCT p.partition_id) as total_partitions,
                COUNT(DISTINCT s.segment_id) as total_segments,
                COALESCE(SUM(s.row_count), 0) as total_rows,
                COALESCE(SUM(s.compressed_bytes), 0) as total_compressed_bytes,
                COALESCE(SUM(s.uncompressed_bytes), 0) as total_uncompressed_bytes
            FROM {}.partitions p
            LEFT JOIN {}.segments s ON p.partition_id = s.partition_id
        "#, self.schema, self.schema))
            .fetch_one(&self.pool)
            .await?;
        
        Ok(StorageStats {
            total_partitions: row.try_get::<i64, _>("total_partitions")? as u64,
            total_segments: row.try_get::<i64, _>("total_segments")? as u64,
            total_rows: row.try_get::<i64, _>("total_rows")? as u64,
            total_compressed_bytes: row.try_get::<i64, _>("total_compressed_bytes")? as u64,
            total_uncompressed_bytes: row.try_get::<i64, _>("total_uncompressed_bytes")? as u64,
        })
    }
    
    async fn get_last_indexed_sequence(&self) -> Result<Option<u64>> {
        let row = sqlx::query(&format!(r#"
            SELECT value FROM {}.metadata WHERE key = 'last_indexed_sequence'
        "#, self.schema))
            .fetch_optional(&self.pool)
            .await?;
        
        match row {
            Some(row) => Ok(Some(row.try_get::<i64, _>("value")? as u64)),
            None => Ok(None),
        }
    }
    
    async fn set_last_indexed_sequence(&self, sequence: u64) -> Result<()> {
        sqlx::query(&format!(r#"
            INSERT INTO {}.metadata (key, value)
            VALUES ('last_indexed_sequence', $1)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        "#, self.schema))
            .bind(sequence as i64)
            .execute(&self.pool)
            .await?;
        
        Ok(())
    }
    
    // NEW: Control plane monitoring methods
    
    async fn list_segments_by_status(
        &self, 
        storage_status: Option<SegmentStatus>, 
        index_status: Option<SegmentStatus>
    ) -> Result<Vec<SegmentMetadata>> {
        let mut query = format!("SELECT * FROM {}.segments WHERE 1=1", self.schema);
        let mut binds = vec![];
        
        if let Some(status) = storage_status {
            query.push_str(&format!(" AND storage_status = ${}::{}::segment_status", binds.len() + 1, self.schema));
            binds.push(Self::status_to_string(status));
        }
        
        if let Some(status) = index_status {
            query.push_str(&format!(" AND index_status = ${}::{}::segment_status", binds.len() + 1, self.schema));
            binds.push(Self::status_to_string(status));
        }
        
        query.push_str(" ORDER BY created_at");
        
        let mut q = sqlx::query(&query);
        for bind in binds {
            q = q.bind(bind);
        }
        
        let rows = q.fetch_all(&self.pool).await?;
        
        let segments: Result<Vec<_>> = rows.into_iter().map(|row| {
            Ok(SegmentMetadata {
                segment_id: row.try_get("segment_id")?,
                partition_id: row.try_get("partition_id")?,
                file_path: row.try_get("file_path")?,
                index_path: row.try_get("index_path")?,
                row_count: row.try_get::<i64, _>("row_count")? as u64,
                compressed_bytes: row.try_get::<i64, _>("compressed_bytes")? as u64,
                uncompressed_bytes: row.try_get::<i64, _>("uncompressed_bytes")? as u64,
                min_timestamp: row.try_get("min_timestamp")?,
                max_timestamp: row.try_get("max_timestamp")?,
                storage_status: Self::string_to_status(row.try_get("storage_status")?),
                index_status: Self::string_to_status(row.try_get("index_status")?),
                schema_version: row.try_get::<i32, _>("schema_version")? as u32,
                is_cold: row.try_get("is_cold")?,
                is_archived: row.try_get("is_archived")?,
                storage_class: row.try_get("storage_class")?,
                transitioned_at: row.try_get("transitioned_at")?,
                archived_at: row.try_get("archived_at")?,
                restore_status: row.try_get("restore_status")?,
                created_at: row.try_get("created_at")?,
                updated_at: row.try_get("updated_at")?,
            })
        }).collect();
        
        Ok(segments?)
    }
    
    async fn get_segment_lifecycle_stats(&self) -> Result<LifecycleStats> {
        let row = sqlx::query(&format!(r#"
            SELECT 
                COUNT(*) as total_segments,
                COUNT(*) FILTER (WHERE storage_status = 'pending') as pending_storage,
                COUNT(*) FILTER (WHERE storage_status = 'stored') as stored_segments,
                COUNT(*) FILTER (WHERE storage_status = 'failed') as failed_storage,
                COUNT(*) FILTER (WHERE index_status = 'pending') as pending_index,
                COUNT(*) FILTER (WHERE index_status = 'stored') as indexed_segments,
                COUNT(*) FILTER (WHERE index_status = 'failed') as failed_index,
                COUNT(*) FILTER (WHERE index_status = 'stored' AND storage_status != 'stored') as orphaned_indexes,
                COUNT(*) FILTER (WHERE storage_status = 'stored' AND index_status = 'failed') as missing_indexes
            FROM {}.segments
        "#, self.schema))
            .fetch_one(&self.pool)
            .await?;
        
        Ok(LifecycleStats {
            total_segments: row.try_get::<i64, _>("total_segments")? as u64,
            pending_storage: row.try_get::<i64, _>("pending_storage")? as u64,
            stored_segments: row.try_get::<i64, _>("stored_segments")? as u64,
            failed_storage: row.try_get::<i64, _>("failed_storage")? as u64,
            pending_index: row.try_get::<i64, _>("pending_index")? as u64,
            indexed_segments: row.try_get::<i64, _>("indexed_segments")? as u64,
            failed_index: row.try_get::<i64, _>("failed_index")? as u64,
            orphaned_indexes: row.try_get::<i64, _>("orphaned_indexes")? as u64,
            missing_indexes: row.try_get::<i64, _>("missing_indexes")? as u64,
        })
    }
    
    async fn mark_segment_storage_failed(&self, segment_id: &str, error: &str) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        
        // Update segment status
        sqlx::query(&format!(r#"
            UPDATE {}.segments 
            SET storage_status = 'failed'::{}::segment_status
            WHERE segment_id = $1
        "#, self.schema, self.schema))
            .bind(segment_id)
            .execute(&mut *tx)
            .await?;
        
        // Log error
        sqlx::query(&format!(r#"
            INSERT INTO {}.segment_errors (segment_id, error_type, error_message)
            VALUES ($1, 'storage', $2)
        "#, self.schema))
            .bind(segment_id)
            .bind(error)
            .execute(&mut *tx)
            .await?;
        
        tx.commit().await?;
        
        warn!("Marked segment {} storage as failed: {}", segment_id, error);
        Ok(())
    }
    
    async fn mark_segment_index_failed(&self, segment_id: &str, error: &str) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        
        // Update segment status
        sqlx::query(&format!(r#"
            UPDATE {}.segments 
            SET index_status = 'failed'::{}::segment_status
            WHERE segment_id = $1
        "#, self.schema, self.schema))
            .bind(segment_id)
            .execute(&mut *tx)
            .await?;
        
        // Log error
        sqlx::query(&format!(r#"
            INSERT INTO {}.segment_errors (segment_id, error_type, error_message)
            VALUES ($1, 'index', $2)
        "#, self.schema))
            .bind(segment_id)
            .bind(error)
            .execute(&mut *tx)
            .await?;
        
        tx.commit().await?;
        
        warn!("Marked segment {} index as failed: {}", segment_id, error);
        Ok(())
    }
}