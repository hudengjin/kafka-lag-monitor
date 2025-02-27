use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use kafka::error::Error as KafkaError;
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug)]
struct ConsumerGroupLag {
    group_id: String,
    topic_lags: HashMap<String, i64>,
}

async fn get_consumer_groups(brokers: Vec<String>) -> Result<Vec<String>, KafkaError> {
    let consumer = Consumer::from_hosts(brokers)
        .with_group("kafka-lag-monitor".to_string())
        .with_fallback_offset(FetchOffset::Earliest)
        .create()?;
    
    Ok(consumer.groups()?.into_iter().map(|g| g.group_id).collect())
}

async fn get_group_lag(brokers: Vec<String>, group_id: &str) -> Result<ConsumerGroupLag, KafkaError> {
    let consumer = Consumer::from_hosts(brokers.clone())
        .with_group(group_id.to_string())
        .with_offset_storage(Some(GroupOffsetStorage::Kafka))
        .with_fallback_offset(FetchOffset::Earliest)
        .with_socket_timeout(Duration::from_secs(10))
        .create()?;

    let mut topic_lags = HashMap::new();
    
    for topic in consumer.topics() {
        let partitions = consumer.fetch_metadata(&topic, Duration::from_secs(5))?.partitions();
        let mut total_lag = 0;

        for partition in partitions {
            let (low, high) = consumer.fetch_watermarks(&topic, partition.id)?;
            let offset = consumer.fetch_last_stable_offset(&topic, partition.id)?;
            total_lag += high - offset;
        }

        if total_lag > 0 {
            topic_lags.insert(topic.to_string(), total_lag);
        }
    }

    Ok(ConsumerGroupLag {
        group_id: group_id.to_string(),
        topic_lags,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let brokers = vec!["localhost:9092".to_string()];
    
    let groups = get_consumer_groups(brokers.clone()).await?;
    let mut all_lags = Vec::new();

    for group in groups {
        match get_group_lag(brokers.clone(), &group).await {
            Ok(lag) => all_lags.push(lag),
            Err(e) => eprintln!("Error getting lag for group {}: {}", group, e),
        }
    }

    for lag in all_lags {
        println!("Consumer Group: {}", lag.group_id);
        for (topic, lag) in &lag.topic_lags {
            println!("  Topic: {}, Lag: {}", topic, lag);
        }
    }

    Ok(())
}
