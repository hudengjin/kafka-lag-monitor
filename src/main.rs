use anyhow::Result;
use chrono::Local;
use config::Config;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::{ClientConfig, Offset};
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;
use tracing::{error, info, instrument};

#[derive(Debug, Deserialize)]
struct AppConfig {
    kafka: KafkaConfig,
    logging: LoggingConfig,
}

#[derive(Debug, Deserialize)]
struct KafkaConfig {
    brokers: String,
    monitor_interval_sec: u64,
    // group_id_pattern: String,  // No longer used after simplification
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

#[instrument(skip(consumer))]
fn get_group_lag(consumer: &BaseConsumer) -> Result<HashMap<(String, i32), (i64, i64)>> {
    let mut lag_data = HashMap::new();

    // Get currently assigned partitions
    let assignment = consumer.assignment()?;
    for topic_partition in assignment.elements() {
        info!("Topic is : {}", topic_partition.topic());
        let (low, high) = consumer.fetch_watermarks(
            topic_partition.topic(),
            topic_partition.partition(),
            Duration::from_secs(5),
        )?;

        let mut tpl = rdkafka::TopicPartitionList::new();
        tpl.add_partition_offset(
            topic_partition.topic(),
            topic_partition.partition(),
            rdkafka::Offset::Invalid,
        )?;

        let offset_map = consumer.committed(Duration::from_secs(5))?;
        let offset = offset_map.elements().first()
            .and_then(|elem| match elem.offset() {
                Offset::Offset(o) => Some(o),
                _ => None
            })
            .unwrap_or(-1);

        if offset >= 0 {
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

    info!("Kafka brokers: {}", settings.kafka.brokers);

    let consumer: BaseConsumer = client_config.create()?;

    loop {
        info!("Starting consumer lag check at {}", Local::now());

        match get_group_lag(&consumer) {
            Ok(lags) => {
                for ((topic, partition), (offset, lag)) in lags {
                    info!(
                        topic = topic,
                        partition = partition,
                        current_offset = offset,
                        lag = lag,
                        "Partition lag"
                    );
                }
            }
            Err(e) => error!(error = %e, "Failed to get lag data"),
        }

        tokio::time::sleep(Duration::from_secs(settings.kafka.monitor_interval_sec)).await;
    }
}
