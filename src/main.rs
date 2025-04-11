//! Hotdog's main entrypoint
#[macro_use]
extern crate serde_derive;
#[cfg(feature = "simd")]
extern crate simd_json;
#[macro_use]
extern crate strum_macros;

use clap::{App, Arg};
use dipstick::{Input, Prefixed, Statsd};
use tracing::log::*;

use std::sync::Arc;

mod connection;
mod errors;
mod json;
mod merge;
mod parse;
mod rules;
mod serve;
mod serve_plain;
mod serve_tls;
mod settings;
mod sink;
mod status;

use serve::*;
use settings::*;

fn main() -> Result<(), errors::HotdogError> {
    use std::panic;
    // take_hook() returns the default hook in case when a custom one is not set
    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        // invoke the default handler and exit the process
        orig_hook(panic_info);
        std::process::exit(1);
    }));
    smol::block_on(run())
}

async fn run() -> Result<(), errors::HotdogError> {
    pretty_env_logger::init();

    info!("Starting hotdog version {}", env!["CARGO_PKG_VERSION"]);

    let matches = App::new("Hotdog")
        .version(env!("CARGO_PKG_VERSION"))
        .author("R Tyler Croy <rtyler@buoyantdata.com>")
        .about("Forward syslog with ease")
        .arg(
            Arg::with_name("config")
                .short("c")
                .long("config")
                .value_name("FILE")
                .help("Sets a custom config file")
                .default_value("hotdog.yml")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("test")
                .short("t")
                .long("test")
                .value_name("TEST_FILE")
                .help("Test a log file against the configured rules")
                .takes_value(true),
        )
        .get_matches();

    let settings_file = matches.value_of("config").unwrap_or("hotdog.yml");
    let settings = Arc::new(settings::load(settings_file));
    let metrics = Arc::new(
        Statsd::send_to(&settings.global.metrics.statsd)
            .expect("Failed to create Statsd recorder")
            .named("hotdog")
            .metrics(),
    );

    let stats = Arc::new(status::StatsHandler::new(metrics.clone()));
    let stats_sender = stats.tx.clone();

    if let Some(st) = &settings.global.status {
        let _task = smol::spawn(status::status_server(
            format!("{}:{}", st.address, st.port),
            stats.clone(),
        ));
    }

    let _task = smol::spawn(async move {
        stats.runloop().await;
    });

    if let Some(test_file) = matches.value_of("test") {
        return rules::test_rules(test_file, settings).await;
    }

    let addr = format!(
        "{}:{}",
        settings.global.listen.address, settings.global.listen.port
    );
    info!("Listening on: {}", addr);

    let state = ServerState {
        settings: settings.clone(),
        stats: stats_sender,
    };

    match &settings.global.listen.tls {
        TlsType::CertAndKey {
            cert: _,
            key: _,
            ca: _,
        } => {
            info!("Serving in TLS mode");
            let mut server = crate::serve_tls::TlsServer::new(&state);
            server.accept_loop(&addr, state).await
        }
        _ => {
            info!("Serving in plaintext mode");
            let mut server = crate::serve_plain::PlaintextServer {};
            server.accept_loop(&addr, state).await
        }
    }
}
