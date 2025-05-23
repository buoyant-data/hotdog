//!
//! The Kafka module contains all the tooling/code necessary for connecting hotdog to Kafka for
//! sending log lines along as Kafka messages
//!

use async_channel::{Receiver, Sender, bounded};
use dipstick::{InputQueueScope, InputScope};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use serde::Deserialize;
use tracing::log::*;

use std::collections::HashMap;
use std::time::Duration;

use super::{Message, Sink};
use crate::status::Stats;

/// The Kafka [Sink] acts as the primary interface between hotdog and Kafka
///
/// There is no buffering of messages received, instead every single message is relayed to Kafka as
/// it is received.
pub struct Kafka {
    config: Config,
    /*
     * I'm not super thrilled about wrapping the FutureProducer in an option, but it's the only way
     * that I can think to create an effective two-phase construction of this struct between
     * ::new() and the .connect() function
     */
    producer: Option<FutureProducer<DefaultClientContext>>,
    stats: InputQueueScope,
    rx: Receiver<Message>,
    tx: Sender<Message>,
}

#[async_trait::async_trait]
impl Sink for Kafka {
    type Config = Config;

    fn new(config: Config, _schemas: &[crate::settings::Schema], stats: InputQueueScope) -> Self {
        let (tx, rx) = bounded(config.buffer);
        Kafka {
            producer: None,
            config,
            stats,
            tx,
            rx,
        }
    }

    /// get_sender() will return a cloned reference to the sender suitable for tasks or threads to
    /// consume and take ownership of
    fn get_sender(&self) -> Sender<Message> {
        self.tx.clone()
    }

    async fn bootstrap(&mut self) {
        debug!("Bootstrapping the Kafka sink");
        let mut rd_conf = ClientConfig::new();

        for (key, value) in self.config.conf.iter() {
            rd_conf.set(key, value);
        }

        /*
         * Allow our brokers to be defined at runtime overriding the configuration
         */
        if let Ok(broker) = std::env::var("KAFKA_BROKER") {
            rd_conf.set("bootstrap.servers", &broker);
        }

        /*
         * Allow SASL/SCRAM username/password to be set at runtime
         */
        if let Ok(broker) = std::env::var("SASL_SCRAM_USERNAME") {
            rd_conf.set("sasl.username", &broker);
        }
        if let Ok(broker) = std::env::var("SASL_SCRAM_PASSWORD") {
            rd_conf.set("sasl.password", &broker);
        }

        let consumer: BaseConsumer = rd_conf
            .create()
            .expect("Creation of Kafka consumer (for metadata) failed");

        if let Ok(metadata) = consumer.fetch_metadata(None, self.config.timeout_ms) {
            debug!("  Broker count: {}", metadata.brokers().len());
            debug!("  Topics count: {}", metadata.topics().len());
            debug!("  Metadata broker name: {}", metadata.orig_broker_name());
            debug!("  Metadata broker id: {}\n", metadata.orig_broker_id());

            self.producer = Some(
                rd_conf
                    .create()
                    .expect("Failed to create the Kafka producer!"),
            );
            return;
        }

        panic!("Failed to connect to a Kafka broker: {:?}", self.config);
    }

    async fn runloop(&self) {
        debug!("Entering the Kafka runloop");
        if self.producer.is_none() {
            panic!("Cannot enter the runloop() without a valid producer");
        }

        let producer = self.producer.as_ref().unwrap();

        loop {
            if let Ok(kmsg) = self.rx.recv().await {
                debug!("Sending to Kafka: {:?}", kmsg);
                /*
                 * Needed in order to prevent concurrent writers from totally
                 * killing parallel performance
                 */
                smol::future::yield_now().await;

                match kmsg {
                    Message::Data {
                        destination,
                        payload,
                    } => {
                        /* Note, setting the `K` (key) type on FutureRecord to a string
                         * even though we're explicitly not sending a key
                         */
                        let stats = self.stats.clone();
                        let timer = self.stats.timer(&Stats::KafkaMsgSent.to_string());
                        let start_handle = timer.start();
                        let producer = producer.clone();

                        smol::spawn(async move {
                            let record =
                                FutureRecord::<String, String>::to(&destination).payload(&payload);
                            let timeout = Timeout::After(Duration::from_secs(60));
                            /*
                             * Intentionally setting the timeout_ms to -1 here so this blocks forever if the
                             * outbound librdkafka queue is full. This will block up the crossbeam channel
                             * properly and cause messages to begin to be dropped, rather than buffering
                             * "forever" inside of hotdog
                             */
                            match producer.send(record, timeout).await {
                                Ok(_) => {
                                    stats
                                        .counter(
                                            Stats::KafkaMsgSubmitted { topic: destination }.into(),
                                        )
                                        .count(1);
                                    timer.stop(start_handle);
                                }
                                Err((err, _)) => {
                                    match err {
                                        /*
                                         * err_type will be one of RdKafkaError types defined:
                                         * https://docs.rs/rdkafka/0.23.1/rdkafka/error/enum.RDKafkaError.html
                                         */
                                        KafkaError::MessageProduction(err_type) => {
                                            error!(
                                                "Failed to send message to Kafka due to: {}",
                                                err_type
                                            );
                                            stats
                                                .counter(
                                                    Stats::KafkaMsgErrored {
                                                        errcode: metric_name_for(err_type),
                                                    }
                                                    .into(),
                                                )
                                                .count(1);
                                        }
                                        other => {
                                            error!("Failed to send message to Kafka! {other:?}");
                                            stats
                                                .counter(
                                                    Stats::KafkaMsgErrored {
                                                        errcode: format!("{other:?}"),
                                                    }
                                                    .into(),
                                                )
                                                .count(1);
                                        }
                                    }
                                }
                            }
                        })
                        .detach();
                    }
                    // The kafka sink doesn't support any other message types
                    _ => {}
                }
            }
        }
    }
}

/// A simple function for formatting the generated strings from RDKafkaError to be useful as metric
/// names for systems like statsd
fn metric_name_for(err: RDKafkaErrorCode) -> String {
    if let Some(name) = err.to_string().to_lowercase().split(' ').next() {
        return name.to_string();
    }
    String::from("unknown")
}

/// Configuration struct for the hotdog configuration file.
///
/// This will be used by the settings parsing to configur a [Kafka] sink when present
#[derive(Clone, Debug, Default, Deserialize)]
pub struct Config {
    #[serde(default = "kafka_buffer_default")]
    pub buffer: usize,
    #[serde(default = "kafka_timeout_default")]
    pub timeout_ms: Duration,
    pub conf: HashMap<String, String>,
    pub topic: String,
}

/// Return the default size used for the Kafka buffer
fn kafka_buffer_default() -> usize {
    1024
}

/// Return the default timeout for Kafka operations
fn kafka_timeout_default() -> Duration {
    Duration::from_secs(30)
}

#[cfg(test)]
mod tests {
    use super::*;

    /**
     * Test that trying to connect to a nonexistent cluster returns false
     */
    #[test]
    #[should_panic]
    fn test_connect_bad_cluster() {
        let mut conf = HashMap::<String, String>::new();
        conf.insert(
            String::from("bootstrap.servers"),
            String::from("example.com:9092"),
        );
        let mut config = Config::default();
        config.buffer = 1;

        let bucket = dipstick::AtomicBucket::new();
        let stats = InputQueueScope::wrap(bucket.clone(), 100);
        let mut k = Kafka::new(config, &[], stats);
        smol::block_on(k.bootstrap());
    }

    /**
     * Tests for converting RDKafkaError strings into statsd suitable metric strings
     */
    #[test]
    fn test_metric_name_1() {
        assert_eq!(
            "messagetimedout",
            metric_name_for(RDKafkaErrorCode::MessageTimedOut)
        );
    }
    #[test]
    fn test_metric_name_2() {
        assert_eq!(
            "unknowntopic",
            metric_name_for(RDKafkaErrorCode::UnknownTopic)
        );
    }
    #[test]
    fn test_metric_name_3() {
        assert_eq!("readonly", metric_name_for(RDKafkaErrorCode::ReadOnly));
    }
}
