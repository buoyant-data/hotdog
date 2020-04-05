/**
 * hotdog's main
 */
extern crate config;
extern crate dipstick;
extern crate handlebars;
extern crate regex;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_regex;
extern crate syslog_rfc5424;

use async_std::{
    io::BufReader,
    net::{TcpListener, TcpStream, ToSocketAddrs},
    prelude::*,
    sync::Arc,
    task,
};
use dipstick::*;
use handlebars::Handlebars;
use log::*;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::collections::HashMap;
use syslog_rfc5424::parse_message;

mod settings;

use settings::*;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn main() -> Result<()> {
    pretty_env_logger::init();

    let settings = Arc::new(settings::load());
    let metrics = Arc::new(Statsd::send_to(&settings.global.metrics.statsd)
        .expect("Failed to create Statsd recorder")
        .named("hotdog")
        .metrics());

    let addr = format!(
        "{}:{}",
        settings.global.listen.address, settings.global.listen.port
    );
    info!("Listening on: {}", addr);

    let fut = accept_loop(addr, settings.clone(), metrics.clone());
    task::block_on(fut)
}

/**
 * accept_loop will simply create the socket listener and dispatch newly accepted connections to
 * the connection_loop function
 */
async fn accept_loop(addr: impl ToSocketAddrs, settings: Arc<settings::Settings>, metrics: Arc<LockingOutput>) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    let mut incoming = listener.incoming();
    let connection_count = metrics.counter("connections");

    while let Some(stream) = incoming.next().await {
        connection_count.count(1);
        let stream = stream?;
        debug!("Accepting from: {}", stream.peer_addr()?);
        let _handle = task::spawn(connection_loop(stream, settings.clone(), metrics.clone()));
    }
    Ok(())
}

/**
 * connection_loop is responsible for handling incoming syslog streams connections
 *
 */
async fn connection_loop(stream: TcpStream, settings: Arc<settings::Settings>, metrics: Arc<LockingOutput>) -> Result<()> {
    debug!("Connection received: {}", stream.peer_addr()?);
    let reader = BufReader::new(&stream);
    let mut lines = reader.lines();
    let lines_count = metrics.counter("lines");

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &settings.global.kafka.brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let hb = Handlebars::new();

    while let Some(line) = lines.next().await {
        let line = line?;
        debug!("log: {}", line);

        let msg = parse_message(line)?;
        lines_count.count(1);

        let mut continue_rules = true;

        for rule in settings.rules.iter() {
            /*
             * If we have been told to stop processing rules, then it's time to bail on this log
             * message
             */
            if ! continue_rules {
                break;
            }

            // The output buffer that we will ultimately send along to the Kafka service
            let mut output = String::new();
            let mut rule_matches = false;
            let mut hash = HashMap::new();
            hash.insert("msg", String::from(&msg.msg));

            match rule.field {
                settings::Field::Msg => {
                    if let Some(captures) = rule.regex.captures(&msg.msg) {
                        rule_matches = true;

                        for name in rule.regex.capture_names() {
                            if let Some(name) = name {
                                if let Some(value) = captures.name(name) {
                                    hash.insert(name, String::from(value.as_str()));
                                }
                            }
                        }
                    }
                },
                _ => {
                },
            }

            if rule_matches == false {
                break;
            }

            /*
             * Process the actions one the rule has matched
             */
            for action in rule.actions.iter() {
                match action {
                    settings::Action::Replace { template } => {
                        if let Ok(rendered) = hb.render_template(template, &hash) {
                            output = rendered;
                        }
                    },
                    settings::Action::Forward { topic } => {
                        if let Ok(rendered) = hb.render_template(topic, &hash) {
                            info!("action is forward {:?}", rendered);
                            producer.send(
                                FutureRecord::to(&rendered)
                                    .payload(&output)
                                    .key(&output), 0).await;

                        }
                    },
                    settings::Action::Stop => {
                        continue_rules = false;
                    },
                }
            }
        }
    }

    debug!("Connection terminating for {}", stream.peer_addr()?);
    Ok(())
}
