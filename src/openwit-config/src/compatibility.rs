use crate::unified_config::UnifiedConfig;
use crate::legacy;
use anyhow::Result;

/// Compatibility layer to convert between new UnifiedConfig and legacy OpenWitConfig
impl UnifiedConfig {
    /// Create UnifiedConfig from legacy Config
    pub fn from_legacy(legacy_config: &legacy::Config) -> Result<Self> {
        // Create default unified config and populate from legacy
        let mut config = UnifiedConfig::default();
        
        // Set environment (legacy config doesn't have environment field, so we default to local)
        config.environment = "local".to_string();
        
        // Networking configuration
        // Note: Gossip configuration has been removed from the new architecture
        
        // Ingestion sources
        if let Some(source) = &legacy_config.source {
            match source {
                legacy::IngestionSource::Kafka => {
                    config.ingestion.sources.kafka.enabled = true;
                    config.ingestion.sources.grpc.enabled = false;
                    config.ingestion.sources.http.enabled = false;
                }
                legacy::IngestionSource::Grpc => {
                    config.ingestion.sources.kafka.enabled = false;
                    config.ingestion.sources.grpc.enabled = true;
                    config.ingestion.sources.http.enabled = false;
                }
                legacy::IngestionSource::Http => {
                    config.ingestion.sources.kafka.enabled = false;
                    config.ingestion.sources.grpc.enabled = false;
                    config.ingestion.sources.http.enabled = true;
                }
                legacy::IngestionSource::All => {
                    config.ingestion.sources.kafka.enabled = true;
                    config.ingestion.sources.grpc.enabled = true;
                    config.ingestion.sources.http.enabled = true;
                }
            }
        }
        
        // gRPC configuration
        if let Some(grpc) = &legacy_config.grpc {
            if let Some(enabled) = grpc.enabled {
                config.ingestion.sources.grpc.enabled = enabled;
            }
            if let Some(bind) = &grpc.bind {
                let port = bind.split(':').last()
                    .and_then(|p| p.parse::<u16>().ok())
                    .unwrap_or(9000);
                config.ingestion.grpc.port = port;
            }
            if let Some(runtime_size) = grpc.runtime_size {
                config.ingestion.grpc.runtime_size = runtime_size as u32;
            }
            if let Some(tls) = &grpc.tls {
                if let Some(mode) = tls.mode {
                    config.ingestion.grpc.tls.enabled = mode;
                }
                config.ingestion.grpc.tls.cert_path = tls.cert_path.clone();
                config.ingestion.grpc.tls.key_path = tls.key_path.clone();
            }
        }
        
        // HTTP configuration
        if let Some(http) = &legacy_config.http {
            if let Some(enabled) = http.enabled {
                config.ingestion.sources.http.enabled = enabled;
            }
            if let Some(bind) = &http.bind {
                let port = bind.split(':').last()
                    .and_then(|p| p.parse::<u16>().ok())
                    .unwrap_or(3000);
                config.ingestion.http.port = port;
            }
        }
        
        // Kafka configuration
        if let Some(kafka) = &legacy_config.kafka {
            config.ingestion.kafka.brokers = Some(kafka.brokers.clone());
            config.ingestion.kafka.topics = kafka.topics.clone();
            config.ingestion.kafka.group_id = Some(kafka.group_id.clone());
            config.ingestion.kafka.pool_size = kafka.pool_size as u32;
        }
        
        // Storage configuration
        if let Some(storage) = &legacy_config.storage {
            if let Some(azure) = &storage.azure {
                config.storage.backend = "azure".to_string();
                config.storage.azure.enabled = true;
                config.storage.azure.account_name = azure.account_name.clone();
                if let Some(container) = &azure.container_name {
                    config.storage.azure.container_name = container.clone();
                }
            }
            if let Some(concurrent) = storage.concurrent_uploads {
                config.storage.upload.max_concurrent_uploads = concurrent as u32;
            }
            if let Some(timeout) = storage.upload_timeout_seconds {
                config.storage.upload.timeout_seconds = timeout;
            }
        }
        
        // Metastore configuration
        if let Some(metastore) = &legacy_config.metastore {
            config.metastore.backend = metastore.backend.clone();
            if metastore.backend == "postgres" {
                if let Some(conn_str) = &metastore.connection_string {
                    config.metastore.postgres.connection_string = conn_str.clone();
                }
                if let Some(max_conn) = metastore.max_connections {
                    config.metastore.postgres.max_connections = max_conn;
                }
                if let Some(schema) = &metastore.schema_name {
                    config.metastore.postgres.schema_name = schema.clone();
                }
            } else if metastore.backend == "sled" {
                if let Some(path) = &metastore.path {
                    config.metastore.sled.path = path.clone();
                }
            }
        }
        
        // Kubernetes configuration
        if let Some(k8s) = &legacy_config.kubernetes {
            if let Some(enabled) = k8s.enabled {
                config.deployment.kubernetes.enabled = enabled;
            }
            if let Some(ns) = &k8s.namespace {
                config.deployment.kubernetes.namespace = ns.clone();
            }
            if let Some(role) = &k8s.pod_role {
                config.deployment.kubernetes.pod_role = role.clone();
            }
            if let Some(group) = &k8s.node_group {
                config.deployment.kubernetes.node_group = group.clone();
            }
            if let Some(svc) = &k8s.headless_service {
                config.deployment.kubernetes.headless_service = svc.clone();
            }
        }
        
        // Processing configuration from legacy buffer/memtable (if processing is present)
        if let Some(ref mut processing) = config.processing {
            if let Some(buffer) = &legacy_config.buffer {
                processing.pipeline.batch_size = buffer.batch_size as u32;
                // Actors config removed - no longer needed
                // config.actors.flusher.flush_seconds = buffer.flush_seconds as u32;
                // WAL dir removed - no longer needed
            }

            if let Some(memtable) = &legacy_config.mem_table {
                processing.lsm_engine.memtable_size_limit_mb = memtable.max_size_mb as u32;
            }
        }
        
        // Prometheus monitoring removed
        
        // Alerting configuration
        if let Some(alerting) = &legacy_config.alerting {
            config.alerting.endpoint = alerting.alert_url.clone();
            config.alerting.thresholds.kafka_messages_per_sec = alerting.thresholds.kafka_messages_per_sec as u32;
            config.alerting.thresholds.flush_queue_depth = alerting.thresholds.flush_queue_depth as u32;
        }
        
        Ok(config)
    }
    /// Convert UnifiedConfig to legacy OpenWitConfig for backward compatibility
    pub fn to_legacy(&self) -> legacy::OpenWitConfig {
        legacy::OpenWitConfig {
            // Determine ingestion source based on what's enabled
            source: if self.ingestion.sources.kafka.enabled {
                Some(legacy::IngestionSource::Kafka)
            } else if self.ingestion.sources.grpc.enabled {
                Some(legacy::IngestionSource::Grpc)
            } else if self.ingestion.sources.http.enabled {
                Some(legacy::IngestionSource::Http)
            } else {
                None
            },
            
            // Node mode - default to monolith
            node_mode: Some(legacy::NodeMode::Monolith),
            
            // gRPC config
            grpc: if self.ingestion.sources.grpc.enabled {
                Some(legacy::GrpcConfig {
                    enabled: Some(true),
                    bind: Some(format!("0.0.0.0:{}", self.ingestion.grpc.port)),
                    runtime_size: Some(self.ingestion.grpc.runtime_size as usize),
                    tls: if self.ingestion.grpc.tls.enabled {
                        Some(legacy::TlsConfig {
                            mode: Some(true),
                            cert_path: self.ingestion.grpc.tls.cert_path.clone(),
                            key_path: self.ingestion.grpc.tls.key_path.clone(),
                        })
                    } else {
                        None
                    },
                })
            } else {
                None
            },
            
            // HTTP config - simplified from new config
            http: if self.ingestion.sources.http.enabled {
                Some(legacy::HttpConfig {
                    enabled: Some(true),
                    bind: Some(format!("0.0.0.0:{}", self.ingestion.http.port)),
                })
            } else {
                None
            },
            
            // Prometheus config
            prometheus: None, // Monitoring removed
            
            // Gossip config - using default values since gossip is deprecated
            gossip: Some(legacy::GossipConfig {
                self_node_name: "node-1".to_string(),
                listen_addr: "127.0.0.1:7946".to_string(),
                seed_nodes: vec![],
            }),
            
            // Kafka config
            kafka: if self.ingestion.sources.kafka.enabled {
                Some(legacy::KafkaConfig {
                    brokers: self.ingestion.kafka.brokers.clone().unwrap_or_default(),
                    topics: self.ingestion.kafka.topics.clone(),
                    group_id: self.ingestion.kafka.group_id.clone().unwrap_or_else(|| "openwit-consumer".to_string()),
                    pool_size: self.ingestion.kafka.pool_size as usize,
                    client_options: None, // Not mapped from new config
                })
            } else {
                None
            },
            
            // MemTable config - simplified (if processing is present)
            mem_table: self.processing.as_ref().map(|p| legacy::MemTableConfig {
                max_size_mb: p.lsm_engine.memtable_size_limit_mb as usize,
            }),

            // Buffer config - simplified (if processing is present)
            buffer: self.processing.as_ref().map(|p| legacy::BufferConfig {
                batch_size: p.pipeline.batch_size as usize,
                flush_seconds: 60, // Default flush interval since actors config removed
                output_dir: "./data/buffer".to_string(), // Default buffer dir
            }),
            
            // Alerting config - map to legacy structure
            alerting: Some(legacy::AlertingConfig {
                alert_url: self.alerting.endpoint.clone(),
                thresholds: legacy::AlertingThresholds {
                    kafka_messages_per_sec: self.alerting.thresholds.kafka_messages_per_sec as u64,
                    memtable_usage_percent: 80, // Default value
                    flush_queue_depth: self.alerting.thresholds.flush_queue_depth as usize,
                },
            }),
            
            // Kubernetes config
            kubernetes: if self.deployment.kubernetes.enabled {
                Some(legacy::KubernetesConfig {
                    enabled: Some(true),
                    namespace: Some(self.deployment.kubernetes.namespace.clone()),
                    pod_role: Some(self.deployment.kubernetes.pod_role.clone()),
                    node_group: Some(self.deployment.kubernetes.node_group.clone()),
                    headless_service: Some(self.deployment.kubernetes.headless_service.clone()),
                })
            } else {
                None
            },
            
            // Storage config - simplified
            storage: Some(legacy::StorageConfig {
                azure: if self.storage.backend == "azure" && self.storage.azure.enabled {
                    Some(legacy::AzureConfig {
                        account_name: self.storage.azure.account_name.clone(),
                        container_name: Some(self.storage.azure.container_name.clone()),
                        access_key: String::new(), // Should be from env var
                    })
                } else {
                    None
                },
                concurrent_uploads: Some(self.storage.upload.max_concurrent_uploads as usize),
                retry_attempts: Some(3),
                upload_timeout_seconds: Some(self.storage.upload.timeout_seconds),
            }),
            
            // Metastore config
            metastore: Some(legacy::MetastoreConfig {
                backend: self.metastore.backend.clone(),
                connection_string: if self.metastore.backend == "postgres" {
                    Some(self.metastore.postgres.connection_string.clone())
                } else {
                    None
                },
                max_connections: Some(self.metastore.postgres.max_connections),
                schema_name: Some(self.metastore.postgres.schema_name.clone()),
                path: if self.metastore.backend == "sled" {
                    Some(self.metastore.sled.path.clone())
                } else {
                    None
                },
            }),
        }
    }
}

// Note: Extension methods for OpenWitConfig are already defined in models.rs

/// Create legacy KafkaConfig from UnifiedConfig
impl UnifiedConfig {
    pub fn to_kafka_config(&self) -> Option<legacy::KafkaConfig> {
        if self.ingestion.sources.kafka.enabled {
            Some(legacy::KafkaConfig {
                brokers: self.ingestion.kafka.brokers.clone().unwrap_or_default(),
                topics: self.ingestion.kafka.topics.clone(),
                group_id: self.ingestion.kafka.group_id.clone().unwrap_or_else(|| "openwit-consumer".to_string()),
                pool_size: self.ingestion.kafka.pool_size as usize,
                client_options: None,
            })
        } else {
            None
        }
    }
}