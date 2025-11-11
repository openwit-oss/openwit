use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use uuid::Uuid;

/// Database client for indexer metadata operations
#[derive(Clone)]
pub struct DbClient {
    pool: PgPool,
}

/// Snapshot metadata for atomic publishing
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Snapshot {
    pub snapshot_id: Uuid,
    pub tenant: String,
    pub signal: String,
    pub level: String, // "delta", "hour", "day"
    pub partition_key: String,
    pub file_ulids: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub published_at: Option<DateTime<Utc>>,
    pub metadata: serde_json::Value,
}

/// Index artifact metadata
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct IndexArtifact {
    pub artifact_id: Uuid,
    pub file_ulid: String,
    pub tenant: String,
    pub signal: String,
    pub partition_key: String,
    pub artifact_type: String, // "zone_map", "bloom", "bitmap", "tantivy"
    pub artifact_url: String,
    pub size_bytes: i64,
    pub created_at: DateTime<Utc>,
    pub metadata: serde_json::Value,
}

/// Data file metadata for tracking ingested files
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct DataFile {
    pub file_ulid: String,
    pub tenant: String,
    pub signal: String,
    pub partition_key: String,
    pub parquet_url: String,
    pub size_bytes: i64,
    pub row_count: i64,
    pub min_timestamp: DateTime<Utc>,
    pub max_timestamp: DateTime<Utc>,
    pub ingested_at: DateTime<Utc>,
    pub indexed_at: Option<DateTime<Utc>>,
    pub metadata: serde_json::Value,
}

/// Coverage manifest for query optimization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoverageManifest {
    pub partition_key: String,
    pub level: String,
    pub manifest_url: Option<String>, // for combined indexes
    pub deltas: Vec<DeltaInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaInfo {
    pub file_ulid: String,
    pub partition_key: String,
    pub zone_map_url: Option<String>,
    pub bloom_url: Option<String>,
    pub bitmap_url: Option<String>,
    pub tantivy_segments: Vec<String>,
}

impl DbClient {
    /// Create new database client
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = PgPool::connect(database_url)
            .await
            .with_context(|| format!("Failed to connect to database: {}", database_url))?;

        let client = Self { pool };
        client.migrate().await?;
        Ok(client)
    }

    /// Run database migrations
    async fn migrate(&self) -> Result<()> {
        // Create indexer schema if it doesn't exist
        sqlx::query(
            r#"
            CREATE SCHEMA IF NOT EXISTS indexer;
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Create snapshots table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS indexer.snapshots (
                snapshot_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                tenant TEXT NOT NULL,
                signal TEXT NOT NULL,
                level TEXT NOT NULL CHECK (level IN ('delta', 'hour', 'day')),
                partition_key TEXT NOT NULL,
                file_ulids TEXT[] NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                published_at TIMESTAMPTZ,
                metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                
                UNIQUE(tenant, signal, level, partition_key, created_at)
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Create index artifacts table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS indexer.index_artifacts (
                artifact_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                file_ulid TEXT NOT NULL,
                tenant TEXT NOT NULL,
                signal TEXT NOT NULL,
                partition_key TEXT NOT NULL,
                artifact_type TEXT NOT NULL CHECK (artifact_type IN ('zone_map', 'bloom', 'bitmap', 'tantivy')),
                artifact_url TEXT NOT NULL,
                size_bytes BIGINT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                
                UNIQUE(file_ulid, artifact_type)
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Create data files table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS indexer.data_files (
                file_ulid TEXT PRIMARY KEY,
                tenant TEXT NOT NULL,
                signal TEXT NOT NULL,
                partition_key TEXT NOT NULL,
                parquet_url TEXT NOT NULL,
                size_bytes BIGINT NOT NULL,
                row_count BIGINT NOT NULL,
                min_timestamp TIMESTAMPTZ NOT NULL,
                max_timestamp TIMESTAMPTZ NOT NULL,
                ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                indexed_at TIMESTAMPTZ,
                metadata JSONB NOT NULL DEFAULT '{}'::jsonb
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Create indexes for performance
        let indexes = vec![
            "CREATE INDEX IF NOT EXISTS idx_snapshots_tenant_signal_level ON indexer.snapshots(tenant, signal, level);",
            "CREATE INDEX IF NOT EXISTS idx_snapshots_published ON indexer.snapshots(published_at) WHERE published_at IS NOT NULL;",
            "CREATE INDEX IF NOT EXISTS idx_artifacts_file_ulid ON indexer.index_artifacts(file_ulid);",
            "CREATE INDEX IF NOT EXISTS idx_artifacts_tenant_signal ON indexer.index_artifacts(tenant, signal);",
            "CREATE INDEX IF NOT EXISTS idx_data_files_tenant_signal ON indexer.data_files(tenant, signal);",
            "CREATE INDEX IF NOT EXISTS idx_data_files_timestamp ON indexer.data_files(min_timestamp, max_timestamp);",
            "CREATE INDEX IF NOT EXISTS idx_data_files_indexed ON indexer.data_files(indexed_at) WHERE indexed_at IS NULL;",
        ];

        for index in indexes {
            sqlx::query(index).execute(&self.pool).await?;
        }

        Ok(())
    }

    /// Insert a new snapshot (atomic publishing unit)
    pub async fn insert_snapshot(&self, snapshot: &Snapshot) -> Result<Uuid> {
        let row = sqlx::query(
            r#"
            INSERT INTO indexer.snapshots 
            (tenant, signal, level, partition_key, file_ulids, metadata)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING snapshot_id
            "#,
        )
        .bind(&snapshot.tenant)
        .bind(&snapshot.signal)
        .bind(&snapshot.level)
        .bind(&snapshot.partition_key)
        .bind(&snapshot.file_ulids)
        .bind(&snapshot.metadata)
        .fetch_one(&self.pool)
        .await?;

        Ok(row.get::<Uuid, _>("snapshot_id"))
    }

    /// Publish a snapshot (mark as visible)
    pub async fn publish_snapshot(&self, snapshot_id: Uuid) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE indexer.snapshots 
            SET published_at = NOW()
            WHERE snapshot_id = $1
            "#,
        )
        .bind(snapshot_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Insert index artifact metadata
    pub async fn insert_index_artifact(&self, artifact: &IndexArtifact) -> Result<Uuid> {
        let row = sqlx::query(
            r#"
            INSERT INTO indexer.index_artifacts
            (file_ulid, tenant, signal, partition_key, artifact_type, artifact_url, size_bytes, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (file_ulid, artifact_type) DO UPDATE SET
                artifact_url = EXCLUDED.artifact_url,
                size_bytes = EXCLUDED.size_bytes,
                metadata = EXCLUDED.metadata
            RETURNING artifact_id
            "#,
        )
        .bind(&artifact.file_ulid)
        .bind(&artifact.tenant)
        .bind(&artifact.signal)
        .bind(&artifact.partition_key)
        .bind(&artifact.artifact_type)
        .bind(&artifact.artifact_url)
        .bind(artifact.size_bytes)
        .bind(&artifact.metadata)
        .fetch_one(&self.pool)
        .await?;

        Ok(row.get::<Uuid, _>("artifact_id"))
    }

    /// List visible delta indexes for a given time range
    pub async fn list_visible_deltas(
        &self,
        tenant: &str,
        signal: &str,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<CoverageManifest>> {
        let from_ts = DateTime::from_timestamp_millis(from_ms).unwrap_or_default();
        let to_ts = DateTime::from_timestamp_millis(to_ms).unwrap_or_default();

        let rows = sqlx::query(
            r#"
            SELECT DISTINCT 
                df.partition_key,
                df.file_ulid,
                ia_zone.artifact_url as zone_map_url,
                ia_bloom.artifact_url as bloom_url,
                ia_bitmap.artifact_url as bitmap_url
            FROM indexer.data_files df
            LEFT JOIN indexer.index_artifacts ia_zone ON df.file_ulid = ia_zone.file_ulid AND ia_zone.artifact_type = 'zone_map'
            LEFT JOIN indexer.index_artifacts ia_bloom ON df.file_ulid = ia_bloom.file_ulid AND ia_bloom.artifact_type = 'bloom'  
            LEFT JOIN indexer.index_artifacts ia_bitmap ON df.file_ulid = ia_bitmap.file_ulid AND ia_bitmap.artifact_type = 'bitmap'
            WHERE df.tenant = $1 
              AND df.signal = $2
              AND df.max_timestamp >= $3
              AND df.min_timestamp <= $4
              AND df.indexed_at IS NOT NULL
            ORDER BY df.partition_key, df.file_ulid
            "#,
        )
        .bind(tenant)
        .bind(signal)
        .bind(from_ts)
        .bind(to_ts)
        .fetch_all(&self.pool)
        .await?;

        // Group by partition_key
        let mut manifests = HashMap::new();
        
        for row in rows {
            let partition_key: String = row.get::<String, _>("partition_key");
            let delta = DeltaInfo {
                file_ulid: row.get::<String, _>("file_ulid"),
                partition_key: partition_key.clone(),
                zone_map_url: row.get::<Option<String>, _>("zone_map_url"),
                bloom_url: row.get::<Option<String>, _>("bloom_url"),
                bitmap_url: row.get::<Option<String>, _>("bitmap_url"),
                tantivy_segments: Vec::new(), // Simplified - would need separate query for tantivy
            };

            manifests
                .entry(partition_key.clone())
                .or_insert_with(|| CoverageManifest {
                    partition_key,
                    level: "delta".to_string(),
                    manifest_url: None,
                    deltas: Vec::new(),
                })
                .deltas
                .push(delta);
        }

        Ok(manifests.into_values().collect())
    }

    /// List data files without delta indexes (need indexing)
    pub async fn list_data_files_without_deltas(
        &self,
        tenant: &str,
        signal: &str,
        limit: i64,
    ) -> Result<Vec<DataFile>> {
        let rows = sqlx::query(
            r#"
            SELECT file_ulid, tenant, signal, partition_key, parquet_url, 
                   size_bytes, row_count, min_timestamp, max_timestamp, 
                   ingested_at, indexed_at, metadata
            FROM indexer.data_files
            WHERE tenant = $1 
              AND signal = $2 
              AND indexed_at IS NULL
            ORDER BY ingested_at
            LIMIT $3
            "#,
        )
        .bind(tenant)
        .bind(signal)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        
        let mut files = Vec::new();
        for row in rows {
            files.push(DataFile {
                file_ulid: row.get::<String, _>("file_ulid"),
                tenant: row.get::<String, _>("tenant"),
                signal: row.get::<String, _>("signal"),
                partition_key: row.get::<String, _>("partition_key"),
                parquet_url: row.get::<String, _>("parquet_url"),
                size_bytes: row.get::<i64, _>("size_bytes"),
                row_count: row.get::<i64, _>("row_count"),
                min_timestamp: row.get::<DateTime<Utc>, _>("min_timestamp"),
                max_timestamp: row.get::<DateTime<Utc>, _>("max_timestamp"),
                ingested_at: row.get::<DateTime<Utc>, _>("ingested_at"),
                indexed_at: row.get::<Option<DateTime<Utc>>, _>("indexed_at"),
                metadata: row.get::<serde_json::Value, _>("metadata"),
            });
        }

        Ok(files)
    }

    /// Mark data file as indexed
    pub async fn mark_file_indexed(&self, file_ulid: &str) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE indexer.data_files 
            SET indexed_at = NOW()
            WHERE file_ulid = $1
            "#,
        )
        .bind(file_ulid)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Insert or update data file metadata
    pub async fn upsert_data_file(&self, data_file: &DataFile) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO indexer.data_files
            (file_ulid, tenant, signal, partition_key, parquet_url, size_bytes, 
             row_count, min_timestamp, max_timestamp, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (file_ulid) DO UPDATE SET
                parquet_url = EXCLUDED.parquet_url,
                size_bytes = EXCLUDED.size_bytes,
                row_count = EXCLUDED.row_count,
                min_timestamp = EXCLUDED.min_timestamp,
                max_timestamp = EXCLUDED.max_timestamp,
                metadata = EXCLUDED.metadata
            "#,
        )
        .bind(&data_file.file_ulid)
        .bind(&data_file.tenant)
        .bind(&data_file.signal)
        .bind(&data_file.partition_key)
        .bind(&data_file.parquet_url)
        .bind(data_file.size_bytes)
        .bind(data_file.row_count)
        .bind(data_file.min_timestamp)
        .bind(data_file.max_timestamp)
        .bind(&data_file.metadata)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Get published snapshots for time range (for coverage queries)
    pub async fn get_published_snapshots(
        &self,
        tenant: &str,
        signal: &str,
        from_ms: i64,
        to_ms: i64,
        prefer_combined: bool,
    ) -> Result<Vec<Snapshot>> {
        let from_ts = DateTime::from_timestamp_millis(from_ms).unwrap_or_default();
        let to_ts = DateTime::from_timestamp_millis(to_ms).unwrap_or_default();

        let mut query = String::from(
            r#"
            SELECT snapshot_id, tenant, signal, level, partition_key, file_ulids,
                   created_at, published_at, metadata
            FROM indexer.snapshots s
            JOIN indexer.data_files df ON df.file_ulid = ANY(s.file_ulids)
            WHERE s.tenant = $1 
              AND s.signal = $2
              AND s.published_at IS NOT NULL
              AND df.max_timestamp >= $3
              AND df.min_timestamp <= $4
            "#,
        );

        if prefer_combined {
            query.push_str(" AND s.level IN ('day', 'hour')");
        }

        query.push_str(" ORDER BY CASE s.level WHEN 'day' THEN 1 WHEN 'hour' THEN 2 ELSE 3 END, s.created_at DESC");

        let rows = sqlx::query(&query)
            .bind(tenant)
            .bind(signal)
            .bind(from_ts)
            .bind(to_ts)
            .fetch_all(&self.pool)
            .await?;
            
        let mut snapshots = Vec::new();
        for row in rows {
            snapshots.push(Snapshot {
                snapshot_id: row.get::<Uuid, _>("snapshot_id"),
                tenant: row.get::<String, _>("tenant"),
                signal: row.get::<String, _>("signal"),
                level: row.get::<String, _>("level"),
                partition_key: row.get::<String, _>("partition_key"),
                file_ulids: row.get::<Vec<String>, _>("file_ulids"),
                created_at: row.get::<DateTime<Utc>, _>("created_at"),
                published_at: row.get::<Option<DateTime<Utc>>, _>("published_at"),
                metadata: row.get::<serde_json::Value, _>("metadata"),
            });
        }

        Ok(snapshots)
    }

    /// Get database pool for advanced operations
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Health check
    pub async fn health_check(&self) -> Result<HashMap<String, String>> {
        let mut metrics = HashMap::new();

        // Check connection
        let _row = sqlx::query("SELECT 1 as test")
            .fetch_one(&self.pool)
            .await?;
        
        metrics.insert("connection".to_string(), "ok".to_string());
        
        // Get pool stats
        metrics.insert("pool_size".to_string(), self.pool.size().to_string());
        metrics.insert("idle_connections".to_string(), self.pool.num_idle().to_string());

        // Get table counts
        let counts = sqlx::query(
            r#"
            SELECT 
                (SELECT COUNT(*) FROM indexer.snapshots WHERE published_at IS NOT NULL) as published_snapshots,
                (SELECT COUNT(*) FROM indexer.index_artifacts) as artifacts,
                (SELECT COUNT(*) FROM indexer.data_files WHERE indexed_at IS NULL) as unindexed_files
            "#
        )
        .fetch_one(&self.pool)
        .await?;

        metrics.insert("published_snapshots".to_string(), 
                      counts.get::<Option<i64>, _>("published_snapshots").unwrap_or(0).to_string());
        metrics.insert("artifacts".to_string(),
                      counts.get::<Option<i64>, _>("artifacts").unwrap_or(0).to_string());
        metrics.insert("unindexed_files".to_string(),
                      counts.get::<Option<i64>, _>("unindexed_files").unwrap_or(0).to_string());

        Ok(metrics)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use testcontainers::clients::Cli;
    use testcontainers::images::postgres::Postgres;

    #[tokio::test]
    async fn test_db_operations() {
        let docker = Cli::default();
        let pg_container = docker.run(Postgres::default());
        let connection_string = format!(
            "postgres://postgres:postgres@127.0.0.1:{}/postgres",
            pg_container.get_host_port_ipv4(5432)
        );

        let db = DbClient::new(&connection_string).await.unwrap();

        // Test data file insertion
        let data_file = DataFile {
            file_ulid: "test_ulid".to_string(),
            tenant: "test_tenant".to_string(),
            signal: "logs".to_string(),
            partition_key: "2024-01-01-00".to_string(),
            parquet_url: "s3://bucket/file.parquet".to_string(),
            size_bytes: 1024,
            row_count: 100,
            min_timestamp: Utc::now(),
            max_timestamp: Utc::now(),
            ingested_at: Utc::now(),
            indexed_at: None,
            metadata: serde_json::json!({}),
        };

        db.upsert_data_file(&data_file).await.unwrap();

        // Test listing unindexed files
        let unindexed = db.list_data_files_without_deltas("test_tenant", "logs", 10).await.unwrap();
        assert_eq!(unindexed.len(), 1);
        assert_eq!(unindexed[0].file_ulid, "test_ulid");
    }
}