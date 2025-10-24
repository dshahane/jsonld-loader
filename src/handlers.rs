use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use actix_web::{web, HttpResponse, Responder};
use sea_orm_macros::FromQueryResult;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use utoipa::ToSchema;
use crate::ingester::JsonLdSchemaIngester;

#[derive(Clone)]
pub struct AppState {
    pub(crate) ingester: Arc<JsonLdSchemaIngester>,
}

#[derive(serde::Deserialize, ToSchema)]
pub struct IngestParams {
    table_name: String,
    filter_top_classes: Option<bool>,
}

#[derive(serde::Deserialize, ToSchema)]
pub struct UriQuery {
    table_name: String,
    use_cache: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromQueryResult, ToSchema)]
pub struct ClassData {
    pub uri: String,
    pub class_data: Value,        // JSONB
    pub class_type: String,
    // note: we store as JSONB array for dynamic typing; convert to Vec when needed
    pub parent_classes: Value,    // JSON array (Vec<String>) serialized as Value
}

// -----------------------------
// WhereClause for SPARQL-like filters
// -----------------------------
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct WhereClause {
    pub property: String,
    pub operator: String,
    pub value: Value,
}

#[derive(Deserialize, ToSchema)]
pub struct SparqlRequest {
    table_name: String,
    where_clauses: Vec<WhereClause>,
    limit: Option<i64>,
}

#[utoipa::path(
    post,
    path = "/ingest",
    request_body(content = IngestParams, content_type = "application/json"),
    responses(
        (status = 200, description = "Ingested"),
        (status = 500, description = "Error")
    )
)]
pub async fn ingest_endpoint(body: web::Json<IngestParams>, data: web::Data<AppState>) -> impl Responder {
    // NOTE: For file ingestion via path. You might change to multipart upload.
    // Here we assume the client already placed the JSON-LD file on disk and provided `file_path` in the body.
    // For simplicity we read a fixed path or an optional param.
    let file_path = env::var("SCHEMA_JSONLD").unwrap();
    let filter = body.filter_top_classes.unwrap_or(false);
    let svc = &data.ingester;
    web::block(move || {
        // spawn blocking wrapper to call async function from actix web (we're in async already so fine)
        // But ingest_file is async so call it directly.
        Ok::<_, anyhow::Error>(())
    }).await.unwrap();

    // call async ingest
    match svc.ingest_file(file_path.as_str(), &body.table_name, filter).await {
        Ok(_) => Ok(HttpResponse::Ok().json(json!({"status": "ok"}))),
        Err(e) => {
            log::error!("ingest error: {:?}", e);
            Err(actix_web::error::ErrorInternalServerError("Ingest failed"))
        }
    }
}

#[utoipa::path(
    get,
    path = "/class/{uri}",
    params(
        ("uri" = String, Path, description = "URI of class"),
        ("table_name" = String, Query, description = "Table name to query from (optional)"),
    ),
    responses(
        (status = 200, description = "Found", body = ClassData),
        (status = 404, description = "Not found"))
)]
pub async fn get_class_by_uri(path: web::Path<(String,)>, q: web::Query<UriQuery>, data: web::Data<AppState>) -> impl Responder {
    let uri = path.into_inner().0;
    let table = &q.table_name;
    let use_cache = q.use_cache.unwrap_or(true);
    match data.ingester.query_by_uri(&uri, table, use_cache).await {
        Ok(Some(cd)) => Ok(HttpResponse::Ok().json(cd)),
        Ok(None) => Ok(HttpResponse::NotFound().json(json!({"error": "not found"}))),
        Err(e) => {
            log::error!("get_class_by_uri error: {:?}", e);
            Err(actix_web::error::ErrorInternalServerError("Query failed"))
        }
    }
}

#[utoipa::path(
    get,
    path = "/recursive-class/{uri}",
    params(
        ("uri" = String, Path, description = "URI of class"),
        ("table_name" = String, Query, description = "Table name to query from (optional)"),
    ),
    responses(
        (status = 200, description = "Found", body = ClassData),
        (status = 404, description = "Not found"))
)]
pub async fn get_recursive_class_by_uri(path: web::Path<(String,)>, q: web::Query<UriQuery>, data: web::Data<AppState>) -> impl Responder {
    let uri = path.into_inner().0;
    let table = &q.table_name;
    let use_cache = q.use_cache.unwrap_or(true);
    let max_depth = 2;
    match data.ingester.get_recursive_schema_json(&uri, table, max_depth).await {
        Ok(schema_json) => Ok(HttpResponse::Ok().json(schema_json)),
        Err(e) => {
            log::error!("get_recursive_class_by_uri error: {:?}", e);
            Err(actix_web::error::ErrorInternalServerError("Recursive class query failed"))
        }
    }
}


#[utoipa::path(
    get,
    path = "/class_resolved/{uri}",
    params(
        ("uri" = String, Path, description = "URI of class"),
        ("table_name" = String, Query, description = "table name to resolve from (optional)"),
        ("max_depth" = i32, Query, description = "How many levels to expand")),
    responses((status = 200, description = "Found", body = ClassData), (status = 404, description = "Not found"))
)]
pub async fn get_class_resolved(path: web::Path<(String,)>, q: web::Query<HashMap<String,String>>, data: web::Data<AppState>) -> impl Responder {
    let uri = path.into_inner().0;
    let table = q.get("table_name").map(|s| s.as_str()).unwrap_or("jsonld_schema");
    let max_depth = q.get("max_depth").and_then(|s| s.parse::<usize>().ok()).unwrap_or(3);
    match data.ingester.query_with_resolution(&uri, table, max_depth).await {
        Ok(Some(cd)) => Ok(HttpResponse::Ok().json(cd)),
        Ok(None) => Ok(HttpResponse::NotFound().json(json!({"error": "not found"}))),
        Err(e) => {
            log::error!("get_class_resolved error: {:?}", e);
            Err(actix_web::error::ErrorInternalServerError("Query failed"))
        }
    }
}

#[utoipa::path(
    post,
    path = "/sparql_select",
    request_body = SparqlRequest,
    responses(
        (status = 200, description = "OK"),
        (status = 500, description = "Error")
    )
)]
pub async fn sparql_select_endpoint(req: web::Json<SparqlRequest>, data: web::Data<AppState>) -> impl Responder {
    match data.ingester.sparql_select(&req.table_name, &req.where_clauses, None, req.limit).await {
        Ok(rows) => Ok(HttpResponse::Ok().json(rows)),
        Err(e) => {
            log::error!("sparql_select error: {:?}", e);
            Err(actix_web::error::ErrorInternalServerError("Sparql failed"))
        }
    }
}

#[utoipa::path(
    get,
    path = "/properties/{uri}",
    params(
        ("uri" = String, Path, description = "URI of the class"),
        ("table_name" = String, Query, description = "table name to resolve from (optional)"),
    ),
    responses(
        (status = 200, description = "Found", body = Value),
        (status = 404, description = "Not found")
    )
)]
pub async fn get_properties_by_uri(
    path: web::Path<(String,)>,
    q: web::Query<UriQuery>,
    data: web::Data<AppState>,
) -> impl Responder {
    let uri = path.into_inner().0;
    let table = &q.table_name;
    let use_cache = q.use_cache.unwrap_or(true);
    let ingester = data.ingester.clone();

    match ingester.query_by_uri(&uri, table, use_cache).await {
        Ok(Some(cd)) => {
            // Try fetching schema:Person properties
            let person_properties_json = match ingester
                .get_properties_for_class("schema:Person", table)
                .await
            {
                Ok(person_properties) => {
                    let props: Vec<_> = person_properties
                        .into_iter()
                        .map(|prop| {
                            let label = prop
                                .class_data
                                .get("rdfs:label")
                                .and_then(|v| v.as_str())
                                .unwrap_or("[No Label]");
                            json!({
                                "uri": prop.uri,
                                "label": label,
                                "class_data": prop.class_data
                            })
                        })
                        .collect();

                    json!({
                        "count": props.len(),
                        "properties": props
                    })
                }
                Err(e) => json!({
                    "error": "Failed to fetch schema:Person properties",
                    "details": e.to_string()
                }),
            };

            // Combine both into one response
            HttpResponse::Ok().json(json!({
                "data": cd,
                "person_properties": person_properties_json
            }))
        }
        Ok(None) => HttpResponse::NotFound().json(json!({"error": "not found"})),
        Err(e) => {
            log::error!("get_properties_by_uri error: {:?}", e);
            HttpResponse::InternalServerError().json(json!({
                "error": "Query failed",
                "details": e.to_string()
            }))
        }
    }
}

#[utoipa::path(
    get,
    path = "/context",
    responses(
        (status = 200, description = "OK"),
        (status = 500, description = "Error")
    )
)]
pub async fn get_context(data: web::Data<AppState>) -> impl Responder {
    let ingester = data.ingester.clone();
    match ingester.get_jsonld_context().await {
        Ok(context_json) => {
            Ok(HttpResponse::Ok().json(context_json))
        },
        Err(e) => {
            Err(actix_web::error::ErrorInternalServerError("Sparql failed"))
        }
    }
}