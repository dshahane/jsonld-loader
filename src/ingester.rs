use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use anyhow::Context;
use async_recursion::async_recursion;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{PgPool, Row};
use sqlx::postgres::PgRow;
use crate::resolution_cache::ResolutionCache;

pub struct JsonLdSchemaIngester {
    client: PgPool,
    cache: Option<Arc<ResolutionCache>>,
    enable_cache: bool,
    context_cache: Option<Arc<Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct ClassData {
    pub uri: String,
    pub class_data: Value, // Requires `json` feature in sqlx
    pub class_type: String,
    pub parent_classes: Vec<String>, // Requires `array` feature in sqlx
}

#[derive(Debug, Clone)]
pub struct WhereClause {
    pub property: String,
    pub operator: String,
    pub value: Value,
}

impl JsonLdSchemaIngester {
    /// Create a new ingester with database connection
    pub async fn new(
        connection_string: &str,
        enable_cache: bool,
        cache_ttl: i64,
    ) -> anyhow::Result<Self> {
        // Updated to use PgPool::connect
        let client = PgPool::connect(connection_string)
            .await
            .context("Failed to connect and create connection pool")?;

        let cache = if enable_cache {
            Some(Arc::new(ResolutionCache::new(cache_ttl)))
        } else {
            None
        };

        Ok(Self {
            client,
            cache,
            enable_cache,
            context_cache: None,
        })
    }

    /// Create the schema table
    pub async fn create_schema_table(&self, table_name: &str) {
        let commands = vec![
            format!(
                r#"
        CREATE TABLE IF NOT EXISTS {} (
            id SERIAL PRIMARY KEY,
            uri TEXT UNIQUE NOT NULL,
            class_data JSONB NOT NULL,
            class_type TEXT,
            parent_classes TEXT[],
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );"#,
                table_name
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS idx_{}_uri ON {} (uri);",
                table_name, table_name
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS idx_{}_class_data ON {} USING GIN (class_data);",
                table_name, table_name
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS idx_{}_class_type ON {} (class_type);",
                table_name, table_name
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS idx_{}_parent_classes ON {} USING GIN (parent_classes);",
                table_name, table_name
            ),
        ];

        // 2. Execute each command individually
        for command in commands {
            sqlx::query(&command)
                .execute(&self.client)
                .await
                .with_context(|| format!("Failed to execute command: {}", command))
                .expect(format!("Error executing command: {}", command.as_str()).as_str());
        }
    }

    pub async fn drop_schema_table(&self, table_name: &str) -> anyhow::Result<()> {
        let command = format!("DROP TABLE IF EXISTS {} CASCADE;", table_name);

        println!("Dropping table: {}", table_name);

        sqlx::query(&command)
            .execute(&self.client)
            .await
            .with_context(|| format!("Failed to drop table: {}", table_name))?;

        println!("Table dropped successfully.");
        Ok(())
    }


    /// Parse JSON-LD file
    pub fn parse_jsonld_file(&self, file_path: &str) -> anyhow::Result<Vec<Value>> {
        let content = std::fs::read_to_string(file_path)
            .context("Failed to read JSON-LD file")?;

        let data: Value = serde_json::from_str(&content)
            .context("Failed to parse JSON")?;

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

    /// Extract URI from class definition
    fn extract_uri(&self, item: &Value) -> Option<String> {
        item.get("@id")
            .or_else(|| item.get("id"))
            .or_else(|| item.get("uri"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    }

    /// Extract class type
    fn extract_class_type(&self, item: &Value) -> String {
        let type_field = item.get("@type").or_else(|| item.get("type"));

        match type_field {
            // Handle array: just take the first string found (best effort)
            Some(Value::Array(arr)) => arr
                .iter()
                .find_map(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            // Handle single string type
            Some(Value::String(s)) => s.clone(),
            // Handle object type (rare, but possible with JSON-LD)
            Some(Value::Object(obj)) => obj
                .get("@id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            _ => String::new(),
        }
    }

    /// Extract parent classes
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

    /// Process properties and convert object references to URI pointers
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
                        let processed_arr: Vec<Value> = arr
                            .iter()
                            .map(|item| {
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
                            })
                            .collect();
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

    /// Filter top-level classes
    fn filter_top_level_classes(&self, classes: Vec<Value>) -> Vec<Value> {
        let class_types = [
            "rdfs:Class",
            "owl:Class",
            "Class",
            "http://www.w3.org/2000/01/rdf-schema#Class",
            "http://www.w3.org/2002/07/owl#Class",
        ];

        classes
            .into_iter()
            .filter(|item| {
                let class_type = self.extract_class_type(item);
                class_types.iter().any(|ct| class_type.contains(ct))
            })
            .collect()
    }

    /// Insert or update a class
    pub async fn insert_class(
        &self,
        uri: &str,
        class_data: &Value,
        class_type: &str,
        parent_classes: &[String],
        table_name: &str,
    ) -> anyhow::Result<()> {
        let query = format!(
            r#"
            INSERT INTO {} (uri, class_data, class_type, parent_classes)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (uri)
            DO UPDATE SET
                class_data = EXCLUDED.class_data,
                class_type = EXCLUDED.class_type,
                parent_classes = EXCLUDED.parent_classes,
                updated_at = CURRENT_TIMESTAMP
            "#,
            table_name
        );

        // Updated to use sqlx::query().bind().execute()
        sqlx::query(&query)
            .bind(uri)
            .bind(class_data)
            .bind(class_type)
            .bind(parent_classes)
            .execute(&self.client)
            .await?;

        Ok(())
    }

    async fn read_and_parse_file(&self, file_path: &str) -> anyhow::Result<Value> {
        use tokio::fs;

        let content = fs::read_to_string(file_path)
            .await
            .with_context(|| format!("Failed to read JSON-LD file at {}", file_path))?;

        let json_ld: Value = serde_json::from_str(&content)
            .context("Failed to parse JSON-LD content as JSON")?;

        Ok(json_ld)
    }

    /// Ingest JSON-LD file into database
    pub async fn ingest_file(
        &mut self,
        file_path: &str,
        table_name: &str,
        filter_top_classes: bool,
    ) -> anyhow::Result<()> {
        println!("Parsing JSON-LD file: {}", file_path);
        let json_document = self.read_and_parse_file(file_path).await?;
        // Cache the context
        if let Some(context_value) = json_document.get("@context") {
            // Assume you have a way to update the ingester's context_cache field (e.g., if &self is mutable)
            // Since `ingest_file` takes `&self`, you'll need interior mutability (like a Mutex or Cell)
            // or you'd pass the context out. For simplicity, we'll assume a way to cache it.
            // For production, the ingester would take `&mut self` or use `std::sync::Mutex`.
            // We'll proceed assuming the context is now cached for `get_jsonld_context`.
            // In a real application, you'd perform a thread-safe update here.
            // self.context_cache = Some(Arc::new(context_value.clone()));
            self.context_cache = Some(Arc::new(context_value.clone()))
        }

        // Extract the list of items to ingest (usually under "@graph")
        let mut items: Vec<Value> = json_document
            .get("@graph")
            .and_then(Value::as_array)
            .cloned() // Clone the array for processing
            .unwrap_or_default();

        if filter_top_classes {
            items = self.filter_top_level_classes(items);
            println!("Found {} class definitions", items.len());
        } else {
            println!("Found {} items", items.len());
        }

        println!("Inserting into database...");
        let mut inserted = 0;

        for item in items {
            let uri = match self.extract_uri(&item) {
                Some(u) => u,
                None => {
                    println!("Warning: Item without URI found, skipping");
                    continue;
                }
            };

            let class_type = self.extract_class_type(&item);
            let parent_classes = self.extract_parent_classes(&item);
            let processed_data = self.process_properties(&item);

            if let Err(e) = self
                .insert_class(&uri, &processed_data, &class_type, &parent_classes, table_name)
                .await
            {
                eprintln!("Error inserting {}: {}", uri, e);
                continue;
            }

            inserted += 1;
            if inserted % 100 == 0 {
                println!("Inserted {} classes...", inserted);
            }
        }

        println!("Successfully inserted {} classes into {}", inserted, table_name);
        Ok(())
    }

    // Method to get a cached context
    pub async fn get_jsonld_context(&self, _file_path: &str) -> anyhow::Result<Value> {
        // If the context is stored/cached in the ingester struct
        if let Some(context_arc) = &self.context_cache {
            // Return the cloned context from the cache
            return Ok(context_arc.as_ref().clone());
        }

        // If context is not cached (e.g., `ingest_file` wasn't called first)
        // You would fall back to reading the file here, but to enforce single read,
        // we'll return an error status saying the context is unavailable.
        Ok(json!({
        "status": "error",
        "message": "Context not available. Ensure 'ingest_file' was called successfully first."
    }))
    }

    /// Query by URI with caching
    pub async fn query_by_uri(
        &self,
        uri: &str,
        table_name: &str,
        use_cache: bool,
    ) -> anyhow::Result<Option<ClassData>> {
        // Check cache first
        if use_cache && self.enable_cache {
            if let Some(cache) = &self.cache {
                if let Some(cached) = cache.get(uri) {
                    return Ok(Some(serde_json::from_value(cached)?));
                }
            }
        }

        let query = format!(
            "SELECT uri, class_data, class_type, parent_classes FROM {} WHERE uri = $1",
            table_name
        );

        // Updated to use sqlx::query_as().bind().fetch_optional()
        let class_data_opt = sqlx::query_as::<_, ClassData>(&query)
            .bind(uri)
            .fetch_optional(&self.client)
            .await?;

        if let Some(class_data) = class_data_opt {
            // Store in cache
            if use_cache && self.enable_cache {
                if let Some(cache) = &self.cache {
                    cache.set(uri.to_string(), serde_json::to_value(&class_data)?);
                }
            }
            Ok(Some(class_data))
        } else {
            Ok(None)
        }
    }

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

                                    if let Ok(Some(referenced)) =
                                        self.query_by_uri(ref_uri, table_name, true).await
                                    {
                                        let resolved_nested = self
                                            .resolve_references(
                                                referenced.class_data,
                                                table_name,
                                                max_depth,
                                                current_depth + 1,
                                                visited,
                                            )
                                            .await?;

                                        map.insert(
                                            key.clone(),
                                            json!({
                                                "_ref": ref_uri,
                                                "_type": "uri_reference",
                                                "_resolved": resolved_nested
                                            }),
                                        );
                                    }
                                }
                            }
                        } else {
                            let resolved = self
                                .resolve_references(
                                    value,
                                    table_name,
                                    max_depth,
                                    current_depth,
                                    visited,
                                )
                                .await?;
                            map.insert(key, resolved);
                        }
                    } else if let Value::Array(arr) = &value {
                        let mut resolved_arr = Vec::new();
                        for item in arr {
                            let resolved = self
                                .resolve_references(
                                    item.clone(),
                                    table_name,
                                    max_depth,
                                    current_depth,
                                    visited,
                                )
                                .await?;
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


    /// Query with automatic resolution
    // ... (This function is unchanged as it only calls query_by_uri and resolve_references)

    pub async fn query_with_resolution(
        &self,
        uri: &str,
        table_name: &str,
        max_depth: usize,
    ) -> anyhow::Result<Option<ClassData>> {
        if let Some(mut result) = self.query_by_uri(uri, table_name, true).await? {
            let mut visited = HashSet::new();
            result.class_data = self
                .resolve_references(result.class_data, table_name, max_depth, 0, &mut visited)
                .await?;
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }


    /// Follow property chain
    // ... (This function is unchanged as it only calls query_by_uri)

    pub async fn follow_property_chain(
        &self,
        uri: &str,
        property_chain: &str,
        table_name: &str,
    ) -> anyhow::Result<Option<Value>> {
        let properties: Vec<&str> = property_chain.split('.').collect();
        let current_data = self.query_by_uri(uri, table_name, true).await?;

        if current_data.is_none() {
            return Ok(None);
        }

        let mut current_value = current_data.unwrap().class_data;

        for prop in properties {
            if let Value::Object(map) = &current_value {
                if let Some(value) = map.get(prop) {
                    current_value = value.clone();

                    // If it's a URI reference, resolve it
                    if let Value::Object(obj) = &current_value {
                        if obj.get("_type").and_then(|v| v.as_str()) == Some("uri_reference") {
                            if let Some(ref_uri) = obj.get("_ref").and_then(|v| v.as_str()) {
                                if let Some(referenced) =
                                    self.query_by_uri(ref_uri, table_name, true).await?
                                {
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

    /// Bulk resolve multiple URIs
    pub async fn bulk_resolve(
        &self,
        uris: &[String],
        table_name: &str,
        max_depth: usize,
    ) -> anyhow::Result<HashMap<String, ClassData>> {
        if uris.is_empty() {
            return Ok(HashMap::new());
        }

        let query = format!(
            "SELECT uri, class_data, class_type, parent_classes FROM {} WHERE uri = ANY($1)",
            table_name
        );

        let fetched_classes: Vec<ClassData> = sqlx::query_as(&query)
            .bind(uris)
            .fetch_all(&self.client)
            .await?;

        let mut uri_map = HashMap::new();

        for class_data in fetched_classes {
            let uri = class_data.uri.clone();

            // Cache individual results
            if self.enable_cache {
                if let Some(cache) = &self.cache {
                    // Use the cloned struct data for caching
                    cache.set(uri.clone(), serde_json::to_value(&class_data)?);
                }
            }

            uri_map.insert(uri, class_data);
        }

        // Resolve references for each
        let mut resolved_map = HashMap::new();
        // Iterate over the input URIs to ensure we resolve all requested ones
        for uri in uris {
            if let Some(mut data) = uri_map.get(uri).cloned() {
                let mut visited = HashSet::new();
                data.class_data = self
                    .resolve_references(data.class_data, table_name, max_depth, 0, &mut visited)
                    .await?;
                resolved_map.insert(uri.clone(), data);
            }
        }

        Ok(resolved_map)
    }

    /// SPARQL-like SELECT query
    pub async fn sparql_select(
        &self,
        table_name: &str,
        where_clauses: &[WhereClause],
        _select_properties: Option<&[String]>, // Unused in this implementation, kept for signature
        limit: Option<i64>,
    ) -> anyhow::Result<Vec<(String, Value)>> {
        let mut query = format!("SELECT uri, class_data FROM {}", table_name);
        let mut conditions = Vec::new();
        let mut param_count = 1;
        let mut params: Vec<Value> = Vec::new(); // Store values to bind later

        for clause in where_clauses {
            let path_parts: Vec<&str> = clause.property.split('.').collect();
            let jsonb_path = if path_parts.len() > 1 {
                let path = path_parts[..path_parts.len() - 1]
                    .iter()
                    .map(|p| format!("'{}'", p))
                    .collect::<Vec<_>>()
                    .join("->");
                format!("class_data->{}->'{}'", path, path_parts.last().unwrap())
            } else {
                format!("class_data->'{}'", path_parts[0])
            };

            // Simplified: This logic must be safe against SQL injection
            // The approach uses dynamic query string creation, which is OK
            // if parameters are ONLY used as bind variables ($N), not inserted directly.
            let condition = match clause.operator.as_str() {
                "=" => format!("{} = to_jsonb(${}::text)", jsonb_path, param_count),
                "!=" => format!("{} != to_jsonb(${}::text)", jsonb_path, param_count),
                "contains" => format!("{}::text ILIKE ${}", jsonb_path, param_count),
                "exists" => format!("{} IS NOT NULL", jsonb_path),
                op @ (">" | "<" | ">=" | "<=") => {
                    format!("({})::numeric {} ${}", jsonb_path, op, param_count)
                }
                _ => continue,
            };

            conditions.push(condition);

            if clause.operator != "exists" {
                // Store the value and increment param_count
                params.push(clause.value.clone());
                param_count += 1;
            }
        }

        if !conditions.is_empty() {
            query.push_str(&format!(" WHERE {}", conditions.join(" AND ")));
        }

        if let Some(lim) = limit {
            query.push_str(&format!(" LIMIT {}", lim));
        }

        // Execute query with collected parameters
        let mut executable_query = sqlx::query(&query);

        // Bind all collected parameters
        for param in &params {
            // Note: sqlx will attempt to convert Value to its type,
            // which can be tricky. It's safer to convert to text/string if possible.
            // Assuming here that the database can accept a string representation of the JSON Value.
            // For robust production code, you'd use a dedicated library for complex query building.
            if let Some(s) = param.as_str() {
                executable_query = executable_query.bind(s);
            } else if param.is_number() {
                // This is a rough type conversion, may fail for large numbers
                if let Some(i) = param.as_i64() {
                    executable_query = executable_query.bind(i);
                } else if let Some(f) = param.as_f64() {
                    executable_query = executable_query.bind(f);
                }
            } else {
                executable_query = executable_query.bind(param.to_string());
            }
        }

        // Execute the query and fetch raw rows
        let rows: Vec<PgRow> = executable_query
            .fetch_all(&self.client)
            .await?;

        // Manually map the raw rows
        let results: Vec<(String, Value)> = rows
            .into_iter()
            .map(|row| {
                let uri: String = row.try_get("uri").unwrap_or_default();
                let data: Value = row.try_get("class_data").unwrap_or_default();
                (uri, data)
            })
            .collect();

        Ok(results)
    }

    /// Find by type
    pub async fn find_by_type(
        &self,
        class_type: &str,
        table_name: &str,
        limit: Option<i64>,
    ) -> anyhow::Result<Vec<ClassData>> {
        let mut query = format!(
            "SELECT uri, class_data, class_type, parent_classes FROM {} WHERE class_type = $1",
            table_name
        );

        if let Some(lim) = limit {
            query.push_str(&format!(" LIMIT {}", lim));
        }

        // Updated to use sqlx::query_as().bind().fetch_all()
        let results = sqlx::query_as(&query)
            .bind(class_type)
            .fetch_all(&self.client)
            .await?;

        Ok(results) // Automatically Vec<ClassData> due to FromRow
    }

    /// Find subclasses
    pub async fn find_subclasses(
        &self,
        parent_uri: &str,
        table_name: &str,
        recursive: bool,
    ) -> anyhow::Result<Vec<ClassData>> {
        if !recursive {
            let query = format!(
                "SELECT uri, class_data, class_type, parent_classes FROM {} WHERE $1 = ANY(parent_classes)",
                table_name
            );

            // Updated to use sqlx::query_as().bind().fetch_all()
            let results = sqlx::query_as(&query)
                .bind(parent_uri)
                .fetch_all(&self.client)
                .await?;

            Ok(results)
        } else {
            let mut all_subclasses = Vec::new();
            let mut visited = HashSet::new();
            let mut to_process = vec![parent_uri.to_string()];

            while let Some(current_uri) = to_process.pop() {
                if visited.contains(&current_uri) {
                    continue;
                }

                visited.insert(current_uri.clone());

                let query = format!(
                    "SELECT uri, class_data, class_type, parent_classes FROM {} WHERE $1 = ANY(parent_classes)",
                    table_name
                );

                // Updated to use sqlx::query_as().bind().fetch_all()
                let fetched_subclasses: Vec<ClassData> = sqlx::query_as(&query)
                    .bind(&current_uri)
                    .fetch_all(&self.client)
                    .await?;

                for subclass in fetched_subclasses {
                    let uri = subclass.uri.clone();
                    all_subclasses.push(subclass);
                    to_process.push(uri);
                }
            }

            Ok(all_subclasses)
        }
    }

    pub async fn get_properties_for_class(
        &self,
        class_uri: &str, // e.g., "schema:Person"
        table_name: &str,
    ) -> anyhow::Result<Vec<ClassData>> {
        let class_record = self.query_by_uri(class_uri, table_name, true).await?;

        let mut domain_uris = HashSet::new();
        domain_uris.insert(class_uri.to_string());

        if let Some(record) = class_record {
            domain_uris.extend(record.parent_classes.into_iter());
        } else {
            eprintln!("Warning: Class URI not found in DB: {}. Returning empty property list.", class_uri);
            return Ok(Vec::default());
        }

        let domain_uris_vec: Vec<String> = domain_uris.into_iter().collect();

        let property_type_curie = "rdf:Property";

        // *** START OF CORRECTED QUERY ***
        let robust_query = format!(
            r#"
            SELECT uri, class_data, class_type, parent_classes FROM {}
            WHERE
                class_type = $1
                AND EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements(
                        -- 1. Check if 'schema:domainIncludes' is an array using jsonb_typeof
                        CASE jsonb_typeof(class_data -> 'schema:domainIncludes')
                            -- 2. If it's an array, use it directly
                            WHEN 'array' THEN class_data -> 'schema:domainIncludes'
                            -- 3. If it's an object, wrap it in a single-element array
                            WHEN 'object' THEN jsonb_build_array(class_data -> 'schema:domainIncludes')
                            -- 4. Otherwise, use an empty array
                            ELSE '[]'::jsonb
                        END
                    ) AS element
                    WHERE
                        -- Check for the CURIE '@id' (e.g., "schema:Order")
                        (element ->> '@id' = ANY($2::text[]))
                        -- OR check for your custom '_ref' (e.g., "schema:Order")
                        OR (element ->> '_ref' = ANY($2::text[]))
                )
            "#,
            table_name
        );
        // *** END OF CORRECTED QUERY ***

        let results = sqlx::query_as::<_, ClassData>(&robust_query)
            .bind(property_type_curie)
            .bind(&domain_uris_vec)
            .fetch_all(&self.client)
            .await
            .context("Failed to execute query to get properties for class")?;

        Ok(results)
    }

    /// Clear cache
    pub fn clear_cache(&self) {
        if let Some(cache) = &self.cache {
            cache.clear();
            println!("Cache cleared");
        }
    }

    /// Get cache statistics
    pub fn get_cache_stats(&self) -> (usize, bool) {
        if let Some(cache) = &self.cache {
            (cache.size(), true)
        } else {
            (0, false)
        }
    }

    fn get_property_info_json(&self, prop: &ClassData) -> Value {
        let label = prop.class_data["rdfs:label"].as_str().unwrap_or("[No Label]").to_string();
        let comment = prop.class_data["rdfs:comment"].as_str().unwrap_or("[No Comment]").to_string();

        // Extract the range (what type the property VALUE should be)
        let range: Vec<String> = prop.class_data["schema:rangeIncludes"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.get("@id").or_else(|| v.get("_ref")).and_then(|id| id.as_str()).map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        json!({
        "uri": prop.uri,
        "label": label,
        "description": comment,
        "rangeIncludes": range,
        })
    }

    async fn get_class_info_json(
        &self,
        class_uri: &str,
        table_name: &str,
        resolved_references: Vec<Value>, // Pass in the recursively resolved references
    ) -> anyhow::Result<Value> {

        let class_record = self.query_by_uri(class_uri, table_name, true).await?;

        let (class_label, class_comment) = match class_record {
            Some(r) => (
                r.class_data["rdfs:label"].as_str().unwrap_or(class_uri).to_string(),
                r.class_data["rdfs:comment"].as_str().unwrap_or("").to_string(),
            ),
            None => ("NOT FOUND".to_string(), "".to_string()),
        };

        // Fetch applicable properties
        let properties = self.get_properties_for_class(class_uri, table_name).await?;

        let properties_json: Vec<Value> = properties
            .into_iter()
            .map(|prop| self.get_property_info_json(&prop))
            .collect();

        Ok(json!({
        "classUri": class_uri,
        "label": class_label,
        "comment": class_comment,
        "properties": properties_json,
        "references": resolved_references, // Insert the list of recursively resolved classes here
        }))
    }

    #[async_recursion::async_recursion]
    async fn get_recursive_schema_inner(
        &self,
        current_uri: &str,
        table_name: &str,
        current_depth: usize,
        max_depth: usize,
        visited: &mut HashSet<String>,
    ) -> anyhow::Result<Value> {
        // If max depth reached OR already visited, return a simple reference object.
        if current_depth > max_depth || visited.contains(current_uri) {
            return Ok(json!({
            "classUri": current_uri,
            "status": if visited.contains(current_uri) { "VISITED" } else { "MAX_DEPTH" }
        }));
        }

        visited.insert(current_uri.to_string());

        // 1. Query the class data to find references
        let class_data_item = match self.query_by_uri(current_uri, table_name, true).await? {
            Some(data) => data.class_data,
            None => {
                return Ok(json!({
                "classUri": current_uri,
                "status": "NOT_FOUND"
            }))
            }
        };

        // 2. Find and process URI references in the class data
        let mut references_to_visit = Vec::new();
        self.collect_references_recursive(&class_data_item, &mut references_to_visit);

        // 3. Recurse on unique references
        let mut resolved_references = Vec::new();
        for ref_uri in references_to_visit {
            let resolved_json = self.get_recursive_schema_inner(
                &ref_uri,
                table_name,
                current_depth + 1,
                max_depth,
                visited,
            ).await?;
            resolved_references.push(resolved_json);
        }

        // 4. Construct and return the full JSON object for the current class
        self.get_class_info_json(current_uri, table_name, resolved_references).await
    }

    pub async fn get_recursive_schema_json(
        &self,
        uri: &str,
        table_name: &str,
        max_depth: usize,
    ) -> anyhow::Result<Value> {
        let mut visited = HashSet::new();
        self.get_recursive_schema_inner(uri, table_name, 0, max_depth, &mut visited).await
    }

    // Helper to collect all references from a JSONB structure.
    fn collect_references_recursive(&self, value: &Value, references: &mut Vec<String>) {
        match value {
            Value::Object(map) => {
                // Check if this object is a URI reference
                if map.get("_type").and_then(|v| v.as_str()) == Some("uri_reference") {
                    if let Some(ref_uri) = map.get("_ref").and_then(|v| v.as_str()) {
                        references.push(ref_uri.to_string());
                    }
                } else {
                    // Recurse into all fields that aren't references themselves
                    for (_, v) in map {
                        self.collect_references_recursive(v, references);
                    }
                }
            }
            Value::Array(arr) => {
                // Recurse into all elements of the array
                for v in arr {
                    self.collect_references_recursive(v, references);
                }
            }
            _ => {} // Base case: primitive value
        }
    }
}
