use crate::types::TopicIndexConfig;
use tracing::{debug, info};
use std::collections::HashSet;
use std::sync::Mutex;

/// Client extraction utilities for Kafka topics
pub struct ClientExtractor;

// Global set to track discovered clients to avoid duplicate logging
lazy_static::lazy_static! {
    static ref DISCOVERED_CLIENTS: Mutex<HashSet<String>> = Mutex::new(HashSet::new());
}

impl ClientExtractor {
    pub fn extract_client_from_topic(topic: &str, config: Option<&TopicIndexConfig>) -> String {
        debug!("Extracting client from topic: {}", topic);
        
        if let Some(index_config) = config {
            for pattern in &index_config.patterns {
                if Self::topic_matches_pattern(topic, &pattern.pattern) {
                    debug!("Topic '{}' matches pattern: {}", topic, pattern.pattern);
                    let parts: Vec<&str> = topic.split(&index_config.client_name_config.topic_separator).collect();
                    
                    // Use index_position for client extraction
                    let client_pos = pattern.index_position;
                    if parts.len() > client_pos {
                        let client = parts[client_pos].to_string();
                        debug!("Extracted client '{}' from index_position {} using pattern '{}'", client, client_pos, pattern.pattern);
                        return client;
                    } else {
                        debug!("Topic '{}' has {} parts, but index_position {} is out of bounds", topic, parts.len(), client_pos);
                    }
                }
            }
            
            debug!("No pattern matched for topic '{}', trying fallback", topic);
        }
        
        // Fallback: use first part of topic
        let parts: Vec<&str> = topic.split('.').collect();
        let client = parts.get(0).unwrap_or(&"unknown").to_string();
        debug!("Using fallback client extraction: '{}'", client);
        client
    }
    
    fn topic_matches_pattern(topic: &str, pattern: &str) -> bool {
        // Convert glob pattern to regex-like matching
        let pattern_parts: Vec<&str> = pattern.split('.').collect();
        let topic_parts: Vec<&str> = topic.split('.').collect();
        
        // If pattern has more parts than topic, it can't match
        if pattern_parts.len() > topic_parts.len() {
            return false;
        }
        
        // Check each part of the pattern
        for (i, pattern_part) in pattern_parts.iter().enumerate() {
            if i >= topic_parts.len() {
                return false;
            }
            
            let topic_part = topic_parts[i];
            
            // Wildcard matches any part
            if *pattern_part == "*" {
                continue;
            }
            
            // Exact match required
            if *pattern_part != topic_part {
                return false;
            }
        }
        
        // For patterns with wildcards, topic can have more parts but must match all non-wildcard parts
        // For exact patterns, topic must have same number of parts
        if pattern.contains("*") {
            // All non-wildcard parts matched, and topic has at least as many parts
            true
        } else {
            // Exact pattern match requires same number of parts
            pattern_parts.len() == topic_parts.len()
        }
    }
    
    /// Validate if a topic matches any configured pattern
    pub fn is_valid_topic(topic: &str, config: Option<&TopicIndexConfig>) -> bool {
        if let Some(index_config) = config {
            for pattern in &index_config.patterns {
                if Self::topic_matches_pattern(topic, &pattern.pattern) {
                    return true;
                }
            }
            // If configuration is provided but no patterns match, topic is invalid
            return false;
        }
        
        // Fallback: when no configuration is provided, allow any topic with at least one dot separator
        topic.contains('.')
    }
    
    /// Extract topic type based on configured patterns
    pub fn extract_topic_type(topic: &str, config: Option<&TopicIndexConfig>) -> Option<String> {
        if let Some(index_config) = config {
            for pattern in &index_config.patterns {
                if Self::topic_matches_pattern(topic, &pattern.pattern) {
                    let parts: Vec<&str> = topic.split(&index_config.client_name_config.topic_separator).collect();
                    
                    // Try to infer type position based on pattern structure
                    // Typically type is one position after client position
                    if parts.len() > (pattern.index_position + 1) {
                        return Some(parts[pattern.index_position + 1].to_string());
                    }
                }
            }
        }
        
        // Fallback: try to extract from common patterns
        let parts: Vec<&str> = topic.split('.').collect();
        if parts.len() >= 4 {
            // Common pattern: prefix.version.client.type.partition
            Some(parts[3].to_string())
        } else {
            None
        }
    }
    
    /// Create a normalized client ID for consistent hashing/routing
    /// This handles special characters and ensures consistency
    pub fn normalize_client_id(client: &str) -> String {
        client
            .to_lowercase()
            .chars()
            .map(|c| {
                if c.is_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect()
    }
    
    /// Check if a client should be auto-created based on topic pattern
    pub fn should_auto_create_client(topic: &str, config: Option<&TopicIndexConfig>) -> bool {
        if let Some(index_config) = config {
            // Check if auto-generation is enabled and topic matches any pattern
            index_config.auto_generate.enabled && Self::is_valid_topic(topic, Some(index_config))
        } else {
            // Fallback: auto-create for any valid topic structure
            topic.contains('.')
        }
    }
    
    /// Log discovered client for monitoring (only log new clients to reduce verbosity)
    pub fn log_client_discovery(topic: &str, client: &str, config: Option<&TopicIndexConfig>) {
        // Check if this client has already been discovered
        if let Ok(mut discovered) = DISCOVERED_CLIENTS.lock() {
            if discovered.contains(client) {
                // Already logged this client, skip
                return;
            }
            
            // Add to discovered set
            discovered.insert(client.to_string());
        }
        
        if let Some(index_config) = config {
            // Find which pattern matched
            for pattern in &index_config.patterns {
                if Self::topic_matches_pattern(topic, &pattern.pattern) {
                    info!(
                        "Discovered NEW client '{}' from topic '{}' (pattern: '{}')",
                        client, topic, pattern.pattern
                    );
                    return;
                }
            }
        }
        
        // Fallback logging
        info!(
            "Discovered NEW client '{}' from topic '{}' (fallback extraction)",
            client, topic
        );
    }
}
