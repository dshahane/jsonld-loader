mod resolution_cache;
mod ingester;
mod handlers;

use crate::ingester::JsonLdSchemaIngester;
use actix_web::{web, App, HttpServer};
use anyhow::{Result};
use std::env;
use std::sync::Arc;
use utoipa::OpenApi;
use crate::handlers::AppState;
use utoipa_swagger_ui::SwaggerUi;

#[derive(OpenApi)]
#[openapi(
    paths(
        crate::handlers::ingest_endpoint,
        crate::handlers::get_class_by_uri,
        crate::handlers::get_class_resolved,
        crate::handlers::sparql_select_endpoint,
        crate::handlers::get_properties_by_uri,
        crate::handlers::get_context,
        crate::handlers::get_recursive_class_by_uri,

    ),
    components(
        schemas(
            crate::handlers::ClassData,
            crate::handlers::WhereClause,
            crate::handlers::SparqlRequest,
            crate::handlers::IngestParams
        )
    ),
    tags(
        (name = "jsonld", description = "JSON-LD schema ingester endpoints")
    )
)]
pub struct ApiDoc;

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    dotenv::dotenv()?;
    // configure via env or hardcode here
    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgres://postgres:password@127.0.0.1/jsonld".to_string());
    let schema_jsonld = env::var("SCHEMA_JSONLD").expect("SCHEMA_JSONLD must be set");
    let table_name = "test_rdfs_classes";

    let ingester = Arc::new(JsonLdSchemaIngester::new(&database_url, true, 3600).await?);

    // Drop table
    ingester.drop_schema_table(table_name).await?;

    // Create table
    ingester.create_schema_table(table_name).await.expect("Unable to create tables");
    ingester
        .ingest_file(schema_jsonld.as_str(), table_name, false)
        .await?;

    let data = web::Data::new(AppState { ingester: ingester.clone() });

    println!("Starting server at http://127.0.0.1:8080 ...");
    HttpServer::new(move || {
        let openapi = ApiDoc::openapi().clone();
        let swagger_ui_service = SwaggerUi::new("/swagger-ui/{tail:.*}")
            .url("/api-doc/openapi.json", openapi).clone();

        App::new()
            .app_data(data.clone())
            .service(web::resource("/ingest").route(web::post().to(crate::handlers::ingest_endpoint)))
            .service(web::resource("/class/{uri}").route(web::get().to(crate::handlers::get_class_by_uri)))
            .service(web::resource("/class_resolved/{uri}").route(web::get().to(crate::handlers::get_class_resolved)))
            .service(web::resource("/properties/{uri}").route(web::get().to(crate::handlers::get_properties_by_uri)))
            .service(web::resource("/context").route(web::get().to(crate::handlers::get_context)))
            .service(web::resource("/recursive-class/{uri}").route(web::get().to(crate::handlers::get_recursive_class_by_uri)))
            .service(web::resource("/sparql_select").route(web::post().to(crate::handlers::sparql_select_endpoint)))
            .service(swagger_ui_service)
    })
        .bind(("127.0.0.1", 8080))?
        .run()
        .await?;

    Ok(())
}

// Example usage
#[tokio::main]
async fn _main_cmd() -> Result<()> {
    dotenv::dotenv()?;

    let connection_string = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let schema_jsonld = env::var("SCHEMA_JSONLD").expect("SCHEMA_JSONLD must be  pub(crate)set");
    let table_name = "test_rdfs_classes";

    // Initialize ingester with caching
    let ingester = JsonLdSchemaIngester::new(connection_string.as_str(), true, 3600).await?;

    // Drop table
    ingester.drop_schema_table(table_name).await?;

    // Create table
    ingester.create_schema_table(table_name).await.expect("Unable to create tables");

    ingester
        .ingest_file(schema_jsonld.as_str(), table_name, false)
        .await?;

    let person_properties = ingester
        .get_properties_for_class("schema:Person", table_name)
        .await?;

    println!("\n--- Properties for schema:Person ({} found) ---", person_properties.len());

    for prop in person_properties {
        // Extract the property label from the class_data JSONB for a nice printout
        let label = prop.class_data["rdfs:label"].as_str().unwrap_or("[No Label]");
        println!("  - {}: {}", label, prop.uri);
    }

    println!("\n=============================================");
    println!("JSON-LD CONTEXT/PREFIX DOCUMENT");
    println!("=================== pub(crate)==========================");

    match ingester.get_jsonld_context().await {
        Ok(context_json) => {
            // Print the resulting JSON structure neatly
            println!("{}", serde_json::to_string_pretty(&context_json)?);
        },
        Err(e) => {
            eprintln!("Error getting JSON-LD Context: {}", e);
        }
    }

    println!("\n=============================================");
    println!("RECURSIVE SCHEMA TRAVERSAL STARTING AT schema:Person");
    println!("=============================================");
    let root_uri = "schema:Product";
    let max_depth = 2;
    match ingester.get_recursive_schema_json(root_uri, table_name, max_depth).await {
        Ok(schema_json) => {
            println!("--- Recursive Schema JSON Output ---");
            // Print the resulting JSON structure neatly
            println!("{}", serde_json::to_string_pretty(&schema_json)?);
            println!("------------------------------------");
        },
        Err(e) => {
            eprintln!("Error during recursive pub(crate) schema generation: {}", e);
        }
    }

    println!("=============================================");

    // ==== Don's work because we don't save instances ===

    // Query with resolution
    if let Some(result) = ingester
        .query_with_resolution("http://example.org/Person", table_name, 2)
        .await?
    {
        println!("Resolved class: {}", result.uri);
        println!("Data: {}", serde_json::to_string_pretty(&result.class_data)?);
    }

    // Follow property chain
    if let Some(city_name) = ingester
        .follow_property_chain(
            "http://example.org/Person#john",
            "address.city.name",
            table_name,
        )
        .await?
    {
        println!("City name: {}", city_name);
    }

    // Bulk resolve
    let uris = vec![
        "http://example.org/Person#john".to_string(),
        "http://example.org/Person#jane".to_string(),
    ];
    let bulk_results = ingester.bulk_resolve(&uris, table_name, 2).await?;
    println!("Bulk resolved {} URIs", bulk_results.len());

    // Find by type
    let persons = ingester.find_by_type("owl:Class", table_name, Some(10)).await?;
    println!("Found {} owl:Class instances", persons.len());

    // Find subclasses
    let subclasses = ingester
        .find_subclasses("http://example.org/Thing", table_name, true)
        .await?;
    println!("Found {} subclasses", subclasses.len());

    // Cache stats
    let (size, enabled) = ingester.get_cache_stats();
    println!("Cache enabled: {}, Size: {}", enabled, size);

    Ok(())
}