use chrono::{DateTime, Duration, Utc};
use dashmap::DashMap;
use serde_json::Value;

pub struct ResolutionCache {
    cache: DashMap<String, (Value, DateTime<Utc>)>,
    ttl: Duration,
}

impl ResolutionCache {
    pub fn new(ttl_seconds: i64) -> Self {
        Self {
            cache: DashMap::new(),
            ttl: Duration::seconds(ttl_seconds),
        }
    }

    pub fn get(&self, key: &str) -> Option<Value> {
        if let Some(entry) = self.cache.get(key) {
            let (value, timestamp) = entry.value();
            if Utc::now() - *timestamp < self.ttl {
                return Some(value.clone());
            } else {
                drop(entry);
                self.cache.remove(key);
            }
        }
        None
    }

    pub fn set(&self, key: String, value: Value) {
        self.cache.insert(key, (value, Utc::now()));
    }

    pub fn clear(&self) {
        self.cache.clear();
    }

    pub fn size(&self) -> usize {
        self.cache.len()
    }
}