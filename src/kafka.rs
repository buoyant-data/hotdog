use crate::status::{Statistic, Stats};
use async_channel::{bounded, Receiver, Sender};
/**
 * The Kafka module contains all the tooling/code necessary for connecting hotdog to Kafka for
 * sending log lines along as Kafka messages
 */
use async_std::task;
use log::*;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use std::collections::HashMap;
use std::convert::TryInto;
use std::time::{Duration, Instant};

/**
 * KafkaMessage just carries a message and its destination topic between tasks
 */
#[derive(Debug)]
pub struct KafkaMessage {
    topic: String,
    msg: String,
}

impl KafkaMessage {
    pub fn new(topic: String, msg: String) -> KafkaMessage {
        KafkaMessage { topic, msg }
    }
}

/**
 * The Kafka struct acts as the primary interface between hotdog and Kafka
 */
pub struct Kafka {
    /*
     * I'm not super thrilled about wrapping the FutureProducer in an option, but it's the only way
     * that I can think to create an effective two-phase construction of this struct between
     * ::new() and the .connect() function
     */
    producer: Option<FutureProducer<DefaultClientContext>>,
    stats: Sender<Statistic>,
    rx: Receiver<KafkaMessage>,
    tx: Sender<KafkaMessage>,
}

impl Kafka {
    pub fn new(message_max: usize, stats: Sender<Statistic>) -> Kafka {
        let (tx, rx) = bounded(message_max);
        Kafka {
            producer: None,
            stats,
            tx,
            rx,
        }
    }

    /**
     * connect() will inherently validate the configuration and perform a blocking call to the
     * configured bootstrap.servers in order to determine whether Kafka is reachable.
     *
     * Assuming Kafka can be reached, the connect() call will construct the producer and return
     * true
     *
     * If timeout_ms is not specified, a default 10s timeout will be used
     */
    pub fn connect(
        &mut self,
        rdkafka_conf: &HashMap<String, String>,
        timeout_ms: Option<Duration>,
    ) -> bool {
        let mut rd_conf = ClientConfig::new();

        for (key, value) in rdkafka_conf.iter() {
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

        let timeout = match timeout_ms {
            Some(ms) => ms,
            None => Duration::from_secs(10),
        };

        if let Ok(metadata) = consumer.fetch_metadata(None, timeout) {
            debug!("  Broker count: {}", metadata.brokers().len());
            debug!("  Topics count: {}", metadata.topics().len());
            debug!("  Metadata broker name: {}", metadata.orig_broker_name());
            debug!("  Metadata broker id: {}\n", metadata.orig_broker_id());

            self.producer = Some(
                rd_conf
                    .create()
                    .expect("Failed to create the Kafka producer!"),
            );

            return true;
        }

        warn!("Failed to connect to a Kafka broker");

        false
    }

    /**
     * get_sender() will return a cloned reference to the sender suitable for tasks or threads to
     * consume and take ownership of
     */
    pub fn get_sender(&self) -> Sender<KafkaMessage> {
        self.tx.clone()
    }

    /**
     * sendloop should be called in a thread/task and will never return
     */
    pub async fn sendloop(&self) -> ! {
        if self.producer.is_none() {
            panic!("Cannot enter the sendloop() without a valid producer");
        }

        let producer = self.producer.as_ref().unwrap();

        loop {
            if let Ok(kmsg) = self.rx.recv().await {
                debug!("Sending to Kafka: {:?}", kmsg);
                /* Note, setting the `K` (key) type on FutureRecord to a string
                 * even though we're explicitly not sending a key
                 */
                let stats = self.stats.clone();

                let start_time = Instant::now();
                let producer = producer.clone();

                /*
                 * Needed in order to prevent concurrent writers from totally
                 * killing parallel performance
                 */
                task::yield_now().await;

                task::spawn(async move {
                    let record = FutureRecord::<String, String>::to(&kmsg.topic).payload(&kmsg.msg);
                    let timeout = Timeout::After(Duration::from_secs(60));
                    /*
                     * Intentionally setting the timeout_ms to -1 here so this blocks forever if the
                     * outbound librdkafka queue is full. This will block up the crossbeam channel
                     * properly and cause messages to begin to be dropped, rather than buffering
                     * "forever" inside of hotdog
                     */
                    match producer.send(record, timeout).await {
                        Ok(_) => {
                            let _ = stats
                                .send((Stats::KafkaMsgSubmitted { topic: kmsg.topic }, 1))
                                .await;
                            /*
                             * dipstick only supports u64 timers anyways, but as_micros() can
                             * give a u128 (!).
                             */
                            if let Ok(elapsed) = start_time.elapsed().as_micros().try_into() {
                                let _ = stats.send((Stats::KafkaMsgSent, elapsed)).await;
                            } else {
                                error!("Could not collect message time because the duration couldn't fit in an i64, yikes");
                            }
                        }
                        Err((err, _)) => {
                            match err {
                                /*
                                 * err_type will be one of RdKafkaError types defined:
                                 * https://docs.rs/rdkafka/0.23.1/rdkafka/error/enum.RDKafkaError.html
                                 */
                                KafkaError::MessageProduction(err_type) => {
                                    error!("Failed to send message to Kafka due to: {}", err_type);
                                    let _ = stats
                                        .send((
                                            Stats::KafkaMsgErrored {
                                                errcode: metric_name_for(err_type),
                                            },
                                            1,
                                        ))
                                        .await;
                                }
                                _ => {
                                    error!("Failed to send message to Kafka!");
                                    let _ = stats
                                        .send((
                                            Stats::KafkaMsgErrored {
                                                errcode: String::from("generic"),
                                            },
                                            1,
                                        ))
                                        .await;
                                }
                            }
                        }
                    }
                });
            }
        }
    }
}

/**
 * A simple function for formatting the generated strings from RDKafkaError to be useful as metric
 * names for systems like statsd
 */
fn metric_name_for(err: RDKafkaErrorCode) -> String {
    if let Some(name) = err.to_string().to_lowercase().split(' ').next() {
        return name.to_string();
    }
    String::from("unknown")
}

#[cfg(test)]
mod tests {
    use super::*;

    /**
     * Test that trying to connect to a nonexistent cluster returns false
     */
    #[test]
    fn test_connect_bad_cluster() {
        let mut conf = HashMap::<String, String>::new();
        conf.insert(
            String::from("bootstrap.servers"),
            String::from("example.com:9092"),
        );
        let (unused_sender, _) = bounded(1);

        let mut k = Kafka::new(1, unused_sender);
        assert_eq!(false, k.connect(&conf, Some(Duration::from_secs(1))));
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
