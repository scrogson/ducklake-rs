use ducklake::{config::DuckLakeConfig, database::Database};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::init();

    println!("🦆 DuckLake Library Example");
    println!("===========================");

    // Show configuration examples
    println!("\n📋 Supported database configurations:");
    for (db_type, url, description) in DuckLakeConfig::examples() {
        println!("  {} - {} ({})", db_type, url, description);
    }

    // Test configuration validation
    println!("\n🔧 Testing configuration validation:");

    // Test valid configurations
    let configs = vec![
        ("PostgreSQL", "postgresql://user:pass@localhost/ducklake"),
        ("MySQL", "mysql://user:pass@localhost/ducklake"),
        ("SQLite", "sqlite:./ducklake.db"),
    ];

    for (db_type, url) in configs {
        let config = DuckLakeConfig::new(url.to_string(), "./data".to_string());
        match config.validate() {
            Ok(_) => {
                let detected = config.detect_database_type()?;
                println!("  ✅ {} - Detected as: {:?}", db_type, detected);
            }
            Err(e) => println!("  ❌ {} - Error: {}", db_type, e),
        }
    }

    // Test invalid configuration
    println!("\n🚨 Testing invalid configuration:");
    let invalid_config = DuckLakeConfig::new("invalid://url".to_string(), "./data".to_string());
    match invalid_config.validate() {
        Ok(_) => println!("  Unexpected: Invalid config was accepted"),
        Err(e) => println!("  ✅ Correctly rejected: {}", e),
    }

    // Show environment variable usage
    println!("\n🌍 Environment variables:");
    match std::env::var("DATABASE_URL") {
        Ok(url) => {
            println!("  DATABASE_URL: {}", mask_database_url(&url));
            let config = DuckLakeConfig::from_env()?;
            let db_type = config.detect_database_type()?;
            println!("  Detected type: {:?}", db_type);

            // Only try to connect if explicitly requested
            if std::env::var("DUCKLAKE_CONNECT").is_ok() {
                println!("\n🔌 Attempting database connection...");
                match Database::new(&config).await {
                    Ok(database) => {
                        println!("  ✅ Connected successfully!");

                        if let Ok(_) = database.health_check().await {
                            println!("  💚 Health check passed");
                        }

                        if let Ok(version) = database.get_version().await {
                            println!("  📊 Database version: {}", version);
                        }

                        if let Ok(_) = database.migrate().await {
                            println!("  ✅ Migrations completed");
                        }
                    }
                    Err(e) => println!("  ❌ Connection failed: {}", e),
                }
            } else {
                println!("  💡 Set DUCKLAKE_CONNECT=1 to test database connection");
            }
        }
        Err(_) => {
            println!("  DATABASE_URL not set");
            println!("  💡 Set DATABASE_URL to test configuration loading");
        }
    }

    println!("\n🎉 DuckLake library example completed!");
    println!("\nTo test with a real database:");
    println!("  export DATABASE_URL=\"sqlite:./test.db\"");
    println!("  export DUCKLAKE_CONNECT=1");
    println!("  cargo run --example basic");

    Ok(())
}

/// Mask sensitive parts of database URL for logging
fn mask_database_url(url: &str) -> String {
    if let Some(at_pos) = url.find('@') {
        if let Some(scheme_end) = url.find("://") {
            let scheme = &url[..scheme_end + 3];
            let after_at = &url[at_pos..];
            format!("{}***{}", scheme, after_at)
        } else {
            "***".to_string()
        }
    } else {
        url.to_string()
    }
}
