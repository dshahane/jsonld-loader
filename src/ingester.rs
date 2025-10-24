use anyhow::Context;
use async_recursion::async_recursion;
use futures_util::{FutureExt};
use sea_orm::{Database, DatabaseConnection, sea_query::{Alias, ColumnDef, Expr, OnConflict, Query, Table, Value as SqValue}, ConnectionTrait, JsonValue};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use chrono::Utc;
use futures_util::future::BoxFuture;
use tokio::sync::RwLock;
use crate::resolution_cache::ResolutionCache;
use crate::handlers::*;

// -----------------------------
// Domain types (ClassData)
// -----------------------------


// -----------------------------
// JsonLdSchemaIngester
// -----------------------------
#[derive(Clone)]
pub struct JsonLdSchemaIngester {
    client: DatabaseConnection,
    cache: Option<Arc<ResolutionCache>>,
    enable_cache: bool,
    // RwLock so that methods taking &self can modify it
    context_cache: Arc<RwLock<Option<Value>>>,
}

impl JsonLdSchemaIngester {
    /// Create with SeaORM Database URL (e.g., "postgres://user:pass@host/db")
    pub async fn new(connection_string: &str, enable_cache: bool, cache_ttl_secs: i64) -> anyhow::Result<Self> {
        let client = Database::connect(connection_string)
            .await
            .context("Failed to connect to database")?;

        let cache = if enable_cache {
            Some(Arc::new(ResolutionCache::new(cache_ttl_secs)))
        } else {
            None
        };

        Ok(Self {
            client,
            cache,
            enable_cache,
            context_cache: Arc::new(RwLock::new(None)),
        })
    }

    // -------------------------
    // Schema management
    // -------------------------
    pub async fn create_schema_table(&self, table_name: &str) -> anyhow::Result<()> {
        // Build CREATE TABLE with sea_query
        let create = Table::create()
            .table(Alias::new(table_name))
            .if_not_exists()
            .col(ColumnDef::new(Alias::new("id")).integer().auto_increment().primary_key())
            .col(ColumnDef::new(Alias::new("uri")).string().unique_key().not_null())
            .col(ColumnDef::new(Alias::new("class_data")).json_binary().not_null())
            .col(ColumnDef::new(Alias::new("class_type")).string())
            .col(ColumnDef::new(Alias::new("parent_classes")).json_binary()) // store as JSONB
            .col(ColumnDef::new(Alias::new("created_at")).timestamp().default(Expr::current_timestamp()))
            .col(ColumnDef::new(Alias::new("updated_at")).timestamp().default(Expr::current_timestamp()))
            .to_owned();

        let stmt = self.client.get_database_backend().build(&create);
        self.client.execute(stmt).await.context("create table failed")?;

        // Create indexes (GIN for JSONB)
        // sea_query Index::create doesn't natively accept USING clause; we'll use raw SQL statement
        let idx_uri = format!("CREATE INDEX IF NOT EXISTS idx_{}_uri ON {} (uri);", table_name, table_name);
        let idx_class_data = format!("CREATE INDEX IF NOT EXISTS idx_{}_class_data ON {} USING GIN (class_data);", table_name, table_name);
        let idx_parent_classes = format!("CREATE INDEX IF NOT EXISTS idx_{}_parent_classes ON {} USING GIN (parent_classes);", table_name, table_name);
        let idx_class_type = format!("CREATE INDEX IF NOT EXISTS idx_{}_class_type ON {} (class_type);", table_name, table_name);

        for sql in &[idx_uri, idx_class_data, idx_parent_classes, idx_class_type] {
            let stmt = sea_orm::Statement::from_string(sea_orm::DbBackend::Postgres, sql.to_owned());
            let _ = self.client.execute(stmt).await; // ignore errors for idempotence
        }

        Ok(())
    }

    pub async fn drop_schema_table(&self, table_name: &str) -> anyhow::Result<()> {
        let sql = format!("DROP TABLE IF EXISTS {} CASCADE;", table_name);
        let stmt = sea_orm::Statement::from_string(sea_orm::DbBackend::Postgres, sql);
        self.client.execute(stmt).await.context("drop table failed")?;
        Ok(())
    }

    // -------------------------
    // JSON-LD parsing helpers (same logic)
    // -------------------------
    pub fn parse_jsonld_file(&self, file_path: &str) -> anyhow::Result<Vec<Value>> {
        let content = std::fs::read_to_string(file_path).context("Failed to read JSON-LD file")?;
        let data: Value = serde_json::from_str(&content).context("Failed to parse JSON")?;

        let items = match data {
            Value::Object(ref obj) if obj.contains_key("@graph") => {
                data["@graph"].as_array().unwrap().clone()
            }
            Value::Object(_) => vec![data],
            Value::Array(arr) => arr,
            _ => anyhow::bail!("Unexpected JSON-LD structure"),
        };

        Ok(items)
    }

    fn extract_uri(&self, item: &Value) -> Option<String> {
        item.get("@id")
            .or_else(|| item.get("id"))
            .or_else(|| item.get("uri"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    }

    fn extract_class_type(&self, item: &Value) -> String {
        let type_field = item.get("@type").or_else(|| item.get("type"));
        match type_field {
            Some(Value::Array(arr)) => arr.iter().find_map(|v| v.as_str()).unwrap_or("").to_string(),
            Some(Value::String(s)) => s.clone(),
            Some(Value::Object(obj)) => obj.get("@id").and_then(|v| v.as_str()).unwrap_or("").to_string(),
            _ => String::new(),
        }
    }

    fn extract_parent_classes(&self, item: &Value) -> Vec<String> {
        let mut parents = Vec::new();
        let fields = ["rdfs:subClassOf", "subClassOf", "owl:subClassOf"];
        for field in &fields {
            if let Some(parent) = item.get(field) {
                match parent {
                    Value::Array(arr) => {
                        for p in arr {
                            if let Some(uri) = self.extract_uri(p) {
                                parents.push(uri);
                            } else if let Some(s) = p.as_str() {
                                parents.push(s.to_string());
                            }
                        }
                    }
                    Value::Object(_) => {
                        if let Some(uri) = self.extract_uri(parent) {
                            parents.push(uri);
                        }
                    }
                    Value::String(s) => {
                        parents.push(s.clone());
                    }
                    _ => {}
                }
            }
        }
        parents
    }

    fn process_properties(&self, item: &Value) -> Value {
        match item {
            Value::Object(map) => {
                let mut processed = serde_json::Map::new();
                for (key, value) in map {
                    if key.starts_with('@') {
                        processed.insert(key.clone(), value.clone());
                    } else if let Value::Object(obj) = value {
                        if obj.contains_key("@id") {
                            processed.insert(
                                key.clone(),
                                json!({
                                    "_ref": obj["@id"],
                                    "_type": "uri_reference"
                                }),
                            );
                        } else {
                            processed.insert(key.clone(), self.process_properties(value));
                        }
                    } else if let Value::Array(arr) = value {
                        let processed_arr: Vec<Value> = arr.iter().map(|item| {
                            if let Value::Object(obj) = item {
                                if obj.contains_key("@id") {
                                    json!({
                                        "_ref": obj["@id"],
                                        "_type": "uri_reference"
                                    })
                                } else {
                                    self.process_properties(item)
                                }
                            } else {
                                item.clone()
                            }
                        }).collect();
                        processed.insert(key.clone(), Value::Array(processed_arr));
                    } else {
                        processed.insert(key.clone(), value.clone());
                    }
                }
                Value::Object(processed)
            }
            _ => item.clone(),
        }
    }

    fn filter_top_level_classes(&self, classes: Vec<Value>) -> Vec<Value> {
        let class_types = [
            "rdfs:Class",
            "owl:Class",
            "Class",
            "http://www.w3.org/2000/01/rdf-schema#Class",
            "http://www.w3.org/2002/07/owl#Class",
        ];
        classes.into_iter().filter(|item| {
            let class_type = self.extract_class_type(item);
            class_types.iter().any(|ct| class_type.contains(ct))
        }).collect()
    }

    // -------------------------
    // Insert / Upsert class (ON CONFLICT)
    // -------------------------
    pub async fn insert_class(
        &self,
        uri: &str,
        class_data: &Value,
        class_type: &str,
        parent_classes: &[String],
        table_name: &str,
    ) -> anyhow::Result<()> {
        // Convert parent_classes to JSON array
        let parent_json = JsonValue::Array(
            parent_classes
                .iter()
                .map(|s| JsonValue::String(s.clone()))
                .collect(),
        );

        let insert_q = Query::insert()
            .into_table(Alias::new(table_name))
            .columns([
                Alias::new("uri"),
                Alias::new("class_data"),
                Alias::new("class_type"),
                Alias::new("parent_classes"),
                Alias::new("updated_at"),
            ])
            .values_panic(vec![
                Expr::value(SqValue::String(Some(Box::new(uri.to_string())))),
                Expr::value(SqValue::Json(Some(Box::new(class_data.clone())))),
                Expr::value(SqValue::String(Some(Box::new(class_type.to_string())))),
                Expr::value(SqValue::Json(Some(Box::new(parent_json)))),
                Expr::value(SqValue::ChronoDateTimeUtc(Some(Box::new(Utc::now())))),
            ])
            .on_conflict(
                OnConflict::column(Alias::new("uri"))
                    .update_columns([
                        Alias::new("class_data"),
                        Alias::new("class_type"),
                        Alias::new("parent_classes"),
                        Alias::new("updated_at"),
                    ])
                    .to_owned(),
            )
            .to_owned();

        let stmt = self.client.get_database_backend().build(&insert_q);
        self.client.execute(stmt).await.context("insert_class failed")?;
        Ok(())
    }

    // -------------------------
    // Read & parse file (async)
    // -------------------------
    pub async fn read_and_parse_file(&self, file_path: &str) -> anyhow::Result<Value> {
        let content = tokio::fs::read_to_string(file_path)
            .await
            .with_context(|| format!("Failed to read JSON-LD file at {}", file_path))?;
        let json_ld: Value = serde_json::from_str(&content).context("Failed to parse JSON-LD content")?;
        Ok(json_ld)
    }

    // -------------------------
    // Ingest file (core ingest loop)
    // -------------------------
    pub async fn ingest_file(
        &self,
        file_path: &str,
        table_name: &str,
        filter_top_classes: bool,
    ) -> anyhow::Result<()> {
        log::info!("Parsing JSON-LD file: {}", file_path);
        let json_document = self.read_and_parse_file(file_path).await?;

        if let Some(context_value) = json_document.get("@context") {
            let mut ctx = self.context_cache.write().await;
            *ctx = Some(context_value.clone());
        }

        let mut items: Vec<Value> = json_document.get("@graph").and_then(Value::as_array).cloned().unwrap_or_default();
        if filter_top_classes {
            items = self.filter_top_level_classes(items);
            log::info!("Found {} class definitions", items.len());
        } else {
            log::info!("Found {} items", items.len());
        }

        let mut inserted = 0usize;

        for item in items {
            let uri = match self.extract_uri(&item) {
                Some(u) => u,
                None => {
                    log::warn!("Item without URI found, skipping");
                    continue;
                }
            };

            let class_type = self.extract_class_type(&item);
            let parent_classes = self.extract_parent_classes(&item);
            let processed_data = self.process_properties(&item);

            if let Err(e) = self.insert_class(&uri, &processed_data, &class_type, &parent_classes, table_name).await {
                log::error!("Error inserting {}: {:?}", uri, e);
                continue;
            }

            inserted += 1;
            if inserted % 100 == 0 {
                log::info!("Inserted {} classes...", inserted);
            }
        }

        log::info!("Successfully inserted {} classes into {}", inserted, table_name);
        Ok(())
    }

    // -------------------------
    // get_jsonld_context
    // -------------------------
    pub async fn get_jsonld_context(&self) -> anyhow::Result<Value> {
        let ctx = self.context_cache.read().await;
        if let Some(v) = ctx.as_ref() {
            Ok(v.clone())
        } else {
            Ok(json!({"status": "error", "message": "Context not available. Ensure 'ingest_file' was called successfully first."}))
        }
    }

    // -------------------------
    // Query by URI with caching
    // -------------------------
    pub async fn query_by_uri(&self, uri: &str, table_name: &str, use_cache: bool) -> anyhow::Result<Option<ClassData>> {
        if use_cache && self.enable_cache {
            if let Some(cache) = &self.cache {
                if let Some(cached) = cache.get(uri) {
                    // cached is a serde_json::Value of full ClassData; attempt deserialize
                    let cd: ClassData = serde_json::from_value(cached).context("failed to deserialize cached ClassData")?;
                    return Ok(Some(cd));
                }
            }
        }
        let table_name = table_name.to_string();
        // build select query (dynamic)
        let select_q = Query::select()
            .columns([
                Alias::new("uri"),
                Alias::new("class_data"),
                Alias::new("class_type"),
                Alias::new("parent_classes"),
            ])
            .from(Alias::new(&table_name))
            .and_where(Expr::col(Alias::new("uri")).eq(uri))
            .to_owned();

        let stmt = self.client.get_database_backend().build(&select_q);
        let row_opt = self.client.query_one(stmt).await?;

        if let Some(row) = row_opt {
            // manually map row to ClassData
            // QueryResult provides try_get_by
            let uri_v: String = row.try_get_by("uri")?;
            let class_data_v: Value = row.try_get_by("class_data")?;
            let class_type_v: String = row.try_get_by("class_type")?;
            let parent_classes_v: Value = row.try_get_by("parent_classes")?;

            let class_data = ClassData {
                uri: uri_v.clone(),
                class_data: class_data_v.clone(),
                class_type: class_type_v.clone(),
                parent_classes: parent_classes_v.clone(),
            };

            // cache it
            if use_cache && self.enable_cache {
                if let Some(cache) = &self.cache {
                    let _ = cache.set(uri.to_string(), serde_json::to_value(&class_data)?);
                }
            }

            Ok(Some(class_data))
        } else {
            Ok(None)
        }
    }

    // -------------------------
    // resolve_references (recursive)
    // -------------------------
    #[async_recursion]
    pub async fn resolve_references(
        &self,
        data: Value,
        table_name: &str,
        max_depth: usize,
        current_depth: usize,
        visited: &mut HashSet<String>,
    ) -> anyhow::Result<Value> {
        if current_depth >= max_depth {
            return Ok(data);
        }

        match data {
            Value::Object(mut map) => {
                for (key, value) in map.clone() {
                    if let Value::Object(obj) = &value {
                        if obj.get("_type").and_then(|v| v.as_str()) == Some("uri_reference") {
                            if let Some(ref_uri) = obj.get("_ref").and_then(|v| v.as_str()) {
                                if !visited.contains(ref_uri) {
                                    visited.insert(ref_uri.to_string());
                                    if let Ok(Some(referenced)) = self.query_by_uri(ref_uri, table_name, true).await {
                                        // referenced.class_data is Value
                                        let resolved_nested = self.resolve_references(referenced.class_data, table_name, max_depth, current_depth + 1, visited).await?;
                                        map.insert(key.clone(), json!({
                                            "_ref": ref_uri,
                                            "_type": "uri_reference",
                                            "_resolved": resolved_nested
                                        }));
                                    }
                                }
                            }
                        } else {
                            let resolved = self.resolve_references(value, table_name, max_depth, current_depth, visited).await?;
                            map.insert(key, resolved);
                        }
                    } else if let Value::Array(arr) = &value {
                        let mut resolved_arr = Vec::new();
                        for item in arr {
                            let resolved = self.resolve_references(item.clone(), table_name, max_depth, current_depth, visited).await?;
                            resolved_arr.push(resolved);
                        }
                        map.insert(key, Value::Array(resolved_arr));
                    }
                }
                Ok(Value::Object(map))
            }
            _ => Ok(data),
        }
    }

    // -------------------------
    // query_with_resolution
    // -------------------------
    pub async fn query_with_resolution(&self, uri: &str, table_name: &str, max_depth: usize) -> anyhow::Result<Option<ClassData>> {
        if let Some(mut result) = self.query_by_uri(uri, table_name, true).await? {
            let mut visited = HashSet::new();
            let class_data_value = result.class_data.clone();
            result.class_data = self.resolve_references(class_data_value, table_name, max_depth, 0, &mut visited).await?;
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }

    // -------------------------
    // follow_property_chain
    // -------------------------
    pub async fn follow_property_chain(&self, uri: &str, property_chain: &str, table_name: &str) -> anyhow::Result<Option<Value>> {
        let properties: Vec<&str> = property_chain.split('.').collect();
        let current_data = self.query_by_uri(uri, table_name, true).await?;
        if current_data.is_none() { return Ok(None); }
        let mut current_value = current_data.unwrap().class_data;

        for prop in properties {
            if let Value::Object(map) = &current_value {
                if let Some(value) = map.get(prop) {
                    current_value = value.clone();
                    if let Value::Object(obj) = &current_value {
                        if obj.get("_type").and_then(|v| v.as_str()) == Some("uri_reference") {
                            if let Some(ref_uri) = obj.get("_ref").and_then(|v| v.as_str()) {
                                if let Some(referenced) = self.query_by_uri(ref_uri, table_name, true).await? {
                                    current_value = referenced.class_data;
                                } else {
                                    return Ok(None);
                                }
                            }
                        }
                    }
                } else {
                    return Ok(None);
                }
            } else {
                return Ok(None);
            }
        }
        Ok(Some(current_value))
    }

    // -------------------------
    // bulk_resolve
    // -------------------------
    pub async fn bulk_resolve(&self, uris: &[String], table_name: &str, max_depth: usize) -> anyhow::Result<HashMap<String, ClassData>> {
        if uris.is_empty() {
            return Ok(HashMap::new());
        }
        let table_name = table_name.to_string();
        // Build IN query using sea_query
        let uri_vals: Vec<SqValue> = uris.iter()
            .map(|u| SqValue::String(Some(Box::new(u.clone()))))
            .collect();
        let q = Query::select()
            .columns([Alias::new("uri"), Alias::new("class_data"), Alias::new("class_type"), Alias::new("parent_classes")])
            .from(Alias::new(&table_name))
            .and_where(Expr::col(Alias::new("uri")).is_in(uri_vals))
            .to_owned();
        let stmt = self.client.get_database_backend().build(&q);
        let rows = self.client.query_all(stmt).await?;
        let mut uri_map: HashMap<String, ClassData> = HashMap::new();
        for row in rows {
            let uri_v: String = row.try_get_by("uri")?;
            let class_data_v: Value = row.try_get_by("class_data")?;
            let class_type_v: String = row.try_get_by("class_type")?;
            let parent_classes_v: Value = row.try_get_by("parent_classes")?;

            let cd = ClassData {
                uri: uri_v.clone(),
                class_data: class_data_v.clone(),
                class_type: class_type_v.clone(),
                parent_classes: parent_classes_v.clone(),
            };

            if self.enable_cache {
                if let Some(cache) = &self.cache {
                    let _ = cache.set(uri_v.clone(), serde_json::to_value(&cd)?);
                }
            }

            uri_map.insert(uri_v, cd);
        }

        let mut resolved_map = HashMap::new();
        for u in uris {
            if let Some(mut data) = uri_map.get(u).cloned() {
                let mut visited = HashSet::new();
                data.class_data = self.resolve_references(data.class_data, table_name.as_str(), max_depth, 0, &mut visited).await?;
                resolved_map.insert(u.clone(), data);
            }
        }
        Ok(resolved_map)
    }

    // -------------------------
    // sparql_select (basic implementation)
    // -------------------------
    pub async fn sparql_select(
        &self,
        table_name: &str,
        where_clauses: &[WhereClause],
        _select_properties: Option<&[String]>,
        limit: Option<i64>,
    ) -> anyhow::Result<Vec<(String, Value)>> {
        // We build a raw WHERE clause using parameter binding to avoid SQL injection.
        // For JSONB path navigation we use text fragments with bindings for values.
        let mut conditions = Vec::new();
        let mut params: Vec<String> = Vec::new();

        for clause in where_clauses {
            // property can be nested with dots; we treat last as leaf
            let path_parts: Vec<&str> = clause.property.split('.').collect();
            let jsonb_path = if path_parts.len() > 1 {
                let path = path_parts[..path_parts.len()-1].iter().map(|p| format!("'{}'", p)).collect::<Vec<_>>().join("->");
                format!("class_data->{}->>'{}'", path, path_parts.last().unwrap())
            } else {
                format!("class_data->>'{}'", path_parts[0])
            };

            let sql_cond = match clause.operator.as_str() {
                "=" => {
                    params.push(clause.value.to_string());
                    format!("{} = ${}", jsonb_path, params.len())
                }
                "!=" => {
                    params.push(clause.value.to_string());
                    format!("{} != ${}", jsonb_path, params.len())
                }
                "contains" => {
                    params.push(format!("%{}%", clause.value.as_str().unwrap_or(&clause.value.to_string())));
                    format!("{} ILIKE ${}", jsonb_path, params.len())
                }
                "exists" => format!("{} IS NOT NULL", jsonb_path),
                op @ (">" | "<" | ">=" | "<=") => {
                    params.push(clause.value.to_string());
                    format!("({})::numeric {} ${}", jsonb_path, op, params.len())
                }
                _ => continue,
            };
            conditions.push(sql_cond);
        }

        let mut sql = format!("SELECT uri, class_data FROM {}", table_name);
        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }
        if let Some(lim) = limit {
            sql.push_str(&format!(" LIMIT {}", lim));
        }

        // Build statement and bind parameters as text
        let mut _q = sea_orm::Statement::from_string(sea_orm::DbBackend::Postgres, sql);
        // Unfortunately sea_orm::Statement doesn't accept bound params for ad-hoc statements easily.
        // As a workaround, we'll use Database::query_all with a built raw statement and use format! earlier for params
        // But to remain safe, we will fall back to using `pg` parameter style with sqlx if you prefer.
        // For simplicity here we'll interpolate parameters safely (we previously ensured params are from JSON values)
        // -> NOTE: if you have untrusted input, prefer using sqlx::query with binds for this method.
        // We'll perform a safe-ish interpolation by using quoted literals for params.
        let mut final_sql = format!("SELECT uri, class_data FROM {}", table_name);
        if !conditions.is_empty() {
            // replace ${n} tokens with quoted values
            let mut cond_sql = conditions.join(" AND ");
            for (i, p) in params.iter().enumerate() {
                // escape single quotes
                let esc = p.replace('\'', "''");
                cond_sql = cond_sql.replace(&format!("${}", i+1), &format!("'{}'", esc));
            }
            final_sql.push_str(" WHERE ");
            final_sql.push_str(&cond_sql);
        }
        if let Some(lim) = limit {
            final_sql.push_str(&format!(" LIMIT {}", lim));
        }

        let stmt = sea_orm::Statement::from_string(sea_orm::DbBackend::Postgres, final_sql);
        let rows = self.client.query_all(stmt).await?;
        let mut results = Vec::new();
        for row in rows {
            let uri_v: String = row.try_get_by("uri")?;
            let class_v: Value = row.try_get_by("class_data")?;
            results.push((uri_v, class_v));
        }
        Ok(results)
    }

    // -------------------------
    // find_by_type
    // -------------------------
    pub async fn find_by_type(&self, class_type: &str, table_name: &str, limit: Option<i64>) -> anyhow::Result<Vec<ClassData>> {
        let table_name = table_name.to_string();
        let mut q = Query::select()
            .columns([Alias::new("uri"), Alias::new("class_data"), Alias::new("class_type"), Alias::new("parent_classes")])
            .from(Alias::new(&table_name))
            .and_where(Expr::col(Alias::new("class_type")).eq(class_type))
            .to_owned();

        if let Some(lim) = limit {
            q.limit(lim as u64);
        }

        let stmt = self.client.get_database_backend().build(&q);
        let rows = self.client.query_all(stmt).await?;
        let mut out = Vec::new();
        for row in rows {
            out.push(ClassData {
                uri: row.try_get_by("uri")?,
                class_data: row.try_get_by("class_data")?,
                class_type: row.try_get_by("class_type")?,
                parent_classes: row.try_get_by("parent_classes")?,
            });
        }
        Ok(out)
    }

    // -------------------------
    // find_subclasses
    // -------------------------
    pub fn find_subclasses<'a>(
        &'a self,
        parent_uri: &'a str,
        table_name: &'a str,
        recursive: bool,
    ) -> BoxFuture<'a, anyhow::Result<Vec<ClassData>>> {
        async move {
            if !recursive {
                // base case
                let parent_json = serde_json::to_string(&vec![parent_uri])?;
                let sql = format!(
                    "SELECT uri, class_data, class_type, parent_classes FROM {} WHERE parent_classes @> $1::jsonb",
                    table_name
                );
                let stmt = sea_orm::Statement::from_string(sea_orm::DbBackend::Postgres, sql);
                let sql_final = stmt.sql.replace(
                    "$1::jsonb",
                    &format!("'{}'::jsonb", parent_json.replace('\'', "''")),
                );
                let stmt_final = sea_orm::Statement::from_string(sea_orm::DbBackend::Postgres, sql_final);
                let rows = self.client.query_all(stmt_final).await?;
                let mut out = Vec::new();
                for row in rows {
                    out.push(ClassData {
                        uri: row.try_get_by("uri")?,
                        class_data: row.try_get_by("class_data")?,
                        class_type: row.try_get_by("class_type")?,
                        parent_classes: row.try_get_by("parent_classes")?,
                    });
                }
                Ok(out)
            } else {
                // recursive case
                let mut all = Vec::new();
                let mut visited = HashSet::new();
                let mut to_process = vec![parent_uri.to_string()];

                while let Some(cur) = to_process.pop() {
                    if visited.contains(&cur) {
                        continue;
                    }
                    visited.insert(cur.clone());

                    let fetched = self.find_subclasses(&cur, table_name, false).await?;
                    for sc in fetched {
                        to_process.push(sc.uri.clone());
                        all.push(sc);
                    }
                }

                Ok(all)
            }
        }
            .boxed()
    }


    // -------------------------
    // get_properties_for_class (complex JSONB query)
    // -------------------------
    pub async fn get_properties_for_class(&self, class_uri: &str, table_name: &str) -> anyhow::Result<Vec<ClassData>> {
        let class_record = self.query_by_uri(class_uri, table_name, true).await?;
        let mut domain_uris = HashSet::new();
        domain_uris.insert(class_uri.to_string());
        if let Some(record) = class_record {
            // attempt to deserialize parent_classes Value -> Vec<String>
            if let Ok(parents_vec) = serde_json::from_value::<Vec<String>>(record.parent_classes.clone()) {
                domain_uris.extend(parents_vec.into_iter());
            }
        } else {
            log::warn!("Class {} not found", class_uri);
            return Ok(Vec::new());
        }

        let domain_vec: Vec<String> = domain_uris.into_iter().collect();
        let _domain_json = serde_json::to_string(&domain_vec)?;

        // Build the robust SQL like in your original code
        let sql = format!(
            r#"
            SELECT uri, class_data, class_type, parent_classes FROM {}
            WHERE
                class_type = $1
                AND EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements(
                        CASE jsonb_typeof(class_data -> 'schema:domainIncludes')
                            WHEN 'array' THEN class_data -> 'schema:domainIncludes'
                            WHEN 'object' THEN jsonb_build_array(class_data -> 'schema:domainIncludes')
                            ELSE '[]'::jsonb
                        END
                    ) AS element
                    WHERE (element ->> '@id' = ANY($2::text[]))
                    OR (element ->> '_ref' = ANY($2::text[]))
                )"#,
            table_name
        );

        // For binding convenience we will interpolate domain_vec as text array literal,
        // but ideally you'd bind using sqlx. For now we produce safe literal:
        let domain_array_literal = domain_vec.iter().map(|s| format!("'{}'", s.replace('\'', "''"))).collect::<Vec<_>>().join(", ");
        let sql_final = sql.replace("$1", &format!("'{}'", "rdf:Property")).replace("$2::text[]", &format!("ARRAY[{}]::text[]", domain_array_literal));

        let stmt = sea_orm::Statement::from_string(sea_orm::DbBackend::Postgres, sql_final);
        let rows = self.client.query_all(stmt).await?;
        let mut out = Vec::new();
        for row in rows {
            out.push(ClassData {
                uri: row.try_get_by("uri")?,
                class_data: row.try_get_by("class_data")?,
                class_type: row.try_get_by("class_type")?,
                parent_classes: row.try_get_by("parent_classes")?,
            });
        }
        Ok(out)
    }

    // -------------------------
    // cache utils
    // -------------------------
    pub fn clear_cache(&self) {
        if let Some(cache) = &self.cache {
            cache.clear();
        }
    }
    pub fn get_cache_stats(&self) -> (usize, bool) {
        if let Some(cache) = &self.cache {
            (cache.size(), true)
        } else {
            (0, false)
        }
    }

    // -------------------------
    // recursive schema builder helpers (collect references)
    // -------------------------
    fn collect_references_recursive(&self, value: &Value, references: &mut Vec<String>) {
        match value {
            Value::Object(map) => {
                if map.get("_type").and_then(|v| v.as_str()) == Some("uri_reference") {
                    if let Some(ref_uri) = map.get("_ref").and_then(|v| v.as_str()) {
                        references.push(ref_uri.to_string());
                    }
                } else {
                    for (_k, v) in map {
                        self.collect_references_recursive(v, references);
                    }
                }
            }
            Value::Array(arr) => {
                for v in arr { self.collect_references_recursive(v, references); }
            }
            _ => {}
        }
    }

    #[async_recursion]
    async fn get_recursive_schema_inner(
        &self,
        current_uri: &str,
        table_name: &str,
        current_depth: usize,
        max_depth: usize,
        visited: &mut HashSet<String>,
    ) -> anyhow::Result<Value> {
        if current_depth > max_depth || visited.contains(current_uri) {
            return Ok(json!({"classUri": current_uri, "status": if visited.contains(current_uri) { "VISITED" } else { "MAX_DEPTH" }}));
        }
        visited.insert(current_uri.to_string());

        let class_data_item = match self.query_by_uri(current_uri, table_name, true).await? {
            Some(data) => data.class_data,
            None => return Ok(json!({"classUri": current_uri, "status": "NOT_FOUND"})),
        };

        let mut refs = Vec::new();
        self.collect_references_recursive(&class_data_item, &mut refs);

        let mut resolved_references = Vec::new();
        for r in refs {
            let resolved = self.get_recursive_schema_inner(&r, table_name, current_depth + 1, max_depth, visited).await?;
            resolved_references.push(resolved);
        }

        let properties = self.get_properties_for_class(current_uri, table_name).await?;
        let properties_json: Vec<Value> = properties.into_iter().map(|p| {
            let label = p.class_data.get("rdfs:label").and_then(|v| v.as_str()).unwrap_or("[No Label]").to_string();
            let comment = p.class_data.get("rdfs:comment").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let range = p.class_data.get("schema:rangeIncludes").and_then(|v| v.as_array()).map(|arr| {
                arr.iter().filter_map(|v| v.get("@id").or_else(|| v.get("_ref")).and_then(|id| id.as_str()).map(|s| s.to_string())).collect::<Vec<_>>()
            }).unwrap_or_default();
            json!({
                "uri": p.uri,
                "label": label,
                "description": comment,
                "rangeIncludes": range
            })
        }).collect();

        Ok(json!({
            "classUri": current_uri,
            "label": class_data_item.get("rdfs:label").and_then(|v| v.as_str()).unwrap_or(current_uri).to_string(),
            "comment": class_data_item.get("rdfs:comment").and_then(|v| v.as_str()).unwrap_or("").to_string(),
            "properties": properties_json,
            "references": resolved_references
        }))
    }

    pub async fn get_recursive_schema_json(&self, uri: &str, table_name: &str, max_depth: usize) -> anyhow::Result<Value> {
        let mut visited = HashSet::new();
        self.get_recursive_schema_inner(uri, table_name, 0, max_depth, &mut visited).await
    }
}
