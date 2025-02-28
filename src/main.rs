use anyhow::{Context, Result};
use chrono::Local;
use config::Config;
use rdkafka::admin::{AdminClient, AdminOptions, GroupListing};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::{FromClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::{ClientConfig, Offset};
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;
use tracing::{debug, error, info, instrument};

#[derive(Debug, Deserialize)]
struct AppConfig {
    kafka: KafkaConfig,
    logging: LoggingConfig,
}

#[derive(Debug, Deserialize)]
struct KafkaConfig {
    brokers: String,
    monitor_interval_sec: u64,
    group_id_pattern: String,
    ssl: SslConfig,
}

#[derive(Debug, Deserialize)]
struct SslConfig {
    enabled: bool,
    ca_path: String,
    cert_path: String,
    key_path: String,
}

#[derive(Debug, Deserialize)]
struct LoggingConfig {
    level: String,
    format: String,
}

#[instrument]
async fn get_consumer_groups(client: &AdminClient<DefaultClientContext>) -> Result<Vec<GroupListing>> {
    let groups = client.list_groups(&AdminOptions::new().request_timeout(Some(Duration::from_secs(5))))
        .await
        .context("Failed to list consumer groups")?;

    Ok(groups)
}

#[instrument(skip(consumer))]
fn get_group_lag(consumer: &BaseConsumer, group_id: &str) -> Result<HashMap<(String, i32), (i64, i64)>> {
    let mut lag_data = HashMap::new();
    
    let group_metadata = consumer.fetch_group_metadata(None, Some(Duration::from_secs(5)))?;
    for topic_partition in group_metadata.topic_partitions() {
        let (low, high) = consumer.fetch_watermarks(
            topic_partition.topic(),
            topic_partition.partition(),
            Duration::from_secs(5),
        )?;

        let offset_map = consumer.committed_offsets(
            &[topic_partition.clone()],
            Duration::from_secs(5)
        )?;
        let offset = offset_map.get(topic_partition).cloned().unwrap_or(Offset::Invalid);

        if let Offset::Offset(offset) = offset {
            let lag = high - offset;
            lag_data.insert(
                (topic_partition.topic().to_string(), topic_partition.partition()),
                (offset, lag),
            );
        }
    }

    Ok(lag_data)
}

#[tokio::main]
async fn main() -> Result<()> {
    let settings = Config::builder()
        .add_source(config::File::with_name("config"))
        .build()?
        .try_deserialize::<AppConfig>()?;

    // 初始化日志
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(match settings.logging.level.as_str() {
            "debug" => tracing::Level::DEBUG,
            "warn" => tracing::Level::WARN,
            "error" => tracing::Level::ERROR,
            _ => tracing::Level::INFO,
        })
        .with_ansi(false)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    // 配置Kafka客户端
    let mut client_config = ClientConfig::new();
    client_config
        .set("bootstrap.servers", &settings.kafka.brokers)
        .set_log_level(RDKafkaLogLevel::Debug);

    if settings.kafka.ssl.enabled {
        client_config
            .set("security.protocol", "ssl")
            .set("ssl.ca.location", &settings.kafka.ssl.ca_path)
            .set("ssl.certificate.location", &settings.kafka.ssl.cert_path)
            .set("ssl.key.location", &settings.kafka.ssl.key_path);
    }

    let admin_client: AdminClient<_> = client_config.create()?;
    let consumer: BaseConsumer = client_config.create()?;

    loop {
        info!("Starting consumer lag check at {}", Local::now());
        
        match get_consumer_groups(&admin_client).await {
            Ok(groups) => {
                let group_filter = regex::Regex::new(&settings.kafka.group_id_pattern)
                    .context("Invalid group_id_pattern regex")?;
                
                for group in groups.into_iter().filter(|g| group_filter.is_match(&g.group_id)) {
                    match get_group_lag(&consumer, &group.group_id) {
                        Ok(lags) => {
                            for ((topic, partition), (offset, lag)) in lags {
                                info!(
                                    group = group.group_id,
                                    topic = topic,
                                    partition = partition,
                                    current_offset = offset,
                                    lag = lag,
                                    "Consumer group lag"
                                );
                            }
                        }
                        Err(e) => error!(error = %e, "Failed to get lag for group {}", group.group_id),
                    }
                }
            }
            Err(e) => error!(error = %e, "Failed to fetch consumer groups"),
        }

        tokio::time::sleep(Duration::from_secs(settings.kafka.monitor_interval_sec)).await;
    }
}
