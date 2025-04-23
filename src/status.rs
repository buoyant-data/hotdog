//! This module contains the necessary code to launch the internal status HTTP
//! server when so configured by the administrator
//!
//! The status module is also responsible for dispatching _all_ statsd metrics.

use serde::{Deserialize, Serialize};
use tide::{Body, Request, Response, StatusCode};
use tracing::log::*;

use std::collections::HashMap;

/**
 * HealthResponse is the simple struct used for serializing statistics for the /stats healthcheck
 * endpoint
 */
#[derive(Deserialize, Serialize)]
struct HealthResponse {
    message: String,
    stats: HashMap<String, i64>,
}

/// Launch the simple status/healthcheck server
pub async fn status_server(listen_to: String) -> Result<(), std::io::Error> {
    let mut app = tide::new();
    debug!("Starting the status server on: {}", listen_to);

    app.at("/")
        .get(|_| async move { Ok("hotdog status server") });

    app.at("/stats").get(|_req: Request<()>| async move {
        let health: HashMap<String, String> = HashMap::default();

        let mut res = Response::new(StatusCode::Ok);
        res.set_body(Body::from_json(&health)?);
        Ok(res)
    });

    app.listen(listen_to).await?;
    Ok(())
}

#[derive(IntoStaticStr, Debug, Display, Hash, PartialEq, Eq, EnumString)]
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
