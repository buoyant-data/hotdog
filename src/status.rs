/**
 * This module contains the necessary code to launch the internal status HTTP
 * server when so configured by the administrator
 *
 * The status module is also responsible for dispatching _all_ statsd metrics.
 */
use async_channel::{Receiver, Sender, bounded};
use dashmap::DashMap;
use dipstick::{InputQueueScope, InputScope, StatsdScope};
use serde::{Deserialize, Serialize};
use tide::{Body, Request, Response, StatusCode};
use tracing::log::*;

use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::Arc;

/**
 * HealthResponse is the simple struct used for serializing statistics for the /stats healthcheck
 * endpoint
 */
#[derive(Deserialize, Serialize)]
struct HealthResponse {
    message: String,
    stats: HashMap<String, i64>,
}

/**
 * Launch the status server
 */
pub async fn status_server(listen_to: String) -> Result<(), std::io::Error> {
    let mut app = tide::new();
    debug!("Starting the status server on: {}", listen_to);

    app.at("/")
        .get(|_| async move { Ok("hotdog status server") });

    app.at("/stats").get(|req: Request<()>| async move {
        let health: HashMap<String, String> = HashMap::default();

        let mut res = Response::new(StatusCode::Ok);
        res.set_body(Body::from_json(&health)?);
        Ok(res)
    });

    app.listen(listen_to).await?;
    Ok(())
}

/**
 * Simple type for tracking our statistics as time goes on
 */
type ThreadsafeStats = Arc<DashMap<String, i64>>;
pub type Statistic = (Stats, i64);

pub struct StatsHandler {
    values: ThreadsafeStats,
    mx: InputQueueScope,
    rx: Receiver<Statistic>,
    pub tx: Sender<Statistic>,
}

impl StatsHandler {
    pub fn new(metrics: StatsdScope) -> Self {
        let (tx, rx) = bounded(1_000_000);
        let values = Arc::new(DashMap::default());
        let mx = InputQueueScope::wrap(metrics, 1_000);

        StatsHandler { values, mx, rx, tx }
    }

    /// The runloop will simply read from the channel and record statistics as
    /// they come in
    pub async fn runloop(&self) {
        loop {
            if let Ok((stat, count)) = self.rx.recv().await {
                trace!("Received stat to record: {} - {}", stat, count);

                match stat {
                    Stats::ConnectionCount => {
                        self.handle_gauge(stat, count).await;
                    }
                    Stats::KafkaMsgSent => {
                        self.handle_timer(stat, count).await;
                    }
                    _ => {
                        self.handle_counter(stat, count).await;
                    }
                }
            }
        }
    }

    /// Update the internal map with a new count like it is a gauge
    async fn handle_gauge(&self, stat: Stats, count: i64) {
        let key = &stat.to_string();
        let mut new_count = 0;

        if let Some(mut gauge) = self.values.get_mut(key) {
            (*gauge) += count;
            new_count = *gauge;
        } else {
            self.values.insert(key.to_string(), count);
            new_count = count;
        }
        //self.metrics.gauge(key).value(new_count);
    }

    /// Update the internal map with a new count like it is a counter
    async fn handle_counter(&self, stat: Stats, count: i64) {
        /*
        let key = &stat.to_string();
        let mut new_count = 0;

        if let Some(mut counter) = self.values.get_mut(key) {
            (*counter) += count;
            new_count = *counter;
        }
        else {
            self.values.insert(key.to_string(), count);
        }

        let sized_count: usize = new_count.try_into().expect("Could not convert to usize!");
        //self.metrics.counter(key).count(sized_count);

        /* Handle special case enums which have more data associated */
        match &stat {
            Stats::KafkaMsgSubmitted { topic } => {
                let subkey = &*format!("{}.{}", key, topic);
                //self.metrics.counter(subkey).count(sized_count);
                self.values.insert(subkey.to_string(), new_count);
            }
            Stats::KafkaMsgErrored { errcode } => {
                let subkey = &*format!("{}.{}", key, errcode);
                //self.metrics.counter(subkey).count(sized_count);
                self.values.insert(subkey.to_string(), new_count);
            }
            _ => {}
        };
        */
    }

    /// Update the internal map with the latest timero
    async fn handle_timer(&self, stat: Stats, duration_us: i64) {
        /*
        let key = &stat.to_string();

        if let Ok(duration) = duration_us.try_into() {
            //self.metrics.timer(key).interval_us(duration);
        } else {
            error!("Failed to report timer to statsd with an i64 that couldn't fit into u64");
        }
        self.values.insert(key.to_string(), duration_us);
        */
    }

    /**
     * Take the internal values map and generated a HealthResponse struct for
     * the /stats url to respond with
     */
    async fn healthcheck(&self) -> HealthResponse {
        let mut stats = HashMap::new();

        for entry in self.values.iter() {
            stats.insert(entry.key().clone(), *entry.value());
        }

        HealthResponse {
            message: "You should smile more".into(),
            stats,
        }
    }
}

#[derive(Debug, Display, Hash, PartialEq, Eq)]
pub enum Stats {
    /* Gauges */
    #[strum(serialize = "connections")]
    ConnectionCount,

    /* Counters */
    #[strum(serialize = "lines")]
    LineReceived,
    #[strum(serialize = "kafka.submitted")]
    KafkaMsgSubmitted { topic: String },
    #[strum(serialize = "kafka.producer.error")]
    KafkaMsgErrored { errcode: String },
    #[strum(serialize = "error.log_parse")]
    LogParseError,
    #[strum(serialize = "error.full_internal_queue")]
    FullInternalQueueError,
    #[strum(serialize = "error.topic_parse_failed")]
    TopicParseFailed,
    #[strum(serialize = "error.internal_push_failed")]
    InternalPushError,
    #[strum(serialize = "error.merge_of_invalid_json")]
    MergeInvalidJsonError,
    #[strum(serialize = "error.merge_target_not_json")]
    MergeTargetNotJsonError,

    /* Timers */
    #[strum(serialize = "kafka.producer.sent")]
    KafkaMsgSent,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanity_check_strum_serialize() {
        let s = Stats::ConnectionCount.to_string();
        assert_eq!("connections", s);
    }
}
