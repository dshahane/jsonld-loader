mod resolution_cache;
mod ingester;

use crate::ingester::JsonLdSchemaIngester;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use std::env;

// Example usage
#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv()?;

    let connection_string = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let schema_jsonld = env::var("SCHEMA_JSONLD").expect("SCHEMA_JSONLD must be set");
    let table_name = "test_rdfs_classes";

    // Initialize ingester with caching
    let mut ingester = JsonLdSchemaIngester::new(connection_string.as_str(), true, 3600).await?;

    // Drop table
    ingester.drop_schema_table(table_name).await?;

    // Create table
    ingester.create_schema_table(table_name).await;

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
    println!("=============================================");

    match ingester.get_jsonld_context(schema_jsonld.as_str()).await {
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
            eprintln!("Error during recursive schema generation: {}", e);
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