//! Hotdog's main entrypoint
#[macro_use]
extern crate serde_derive;
#[cfg(feature = "simd")]
extern crate simd_json;
#[macro_use]
extern crate strum_macros;

use clap::{App, Arg};
use dipstick::{Input, InputQueueScope, Prefixed, Statsd};
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

#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

/// Non-async entrypoint which will set up the logger and launch the main smol task
fn main() -> Result<(), errors::HotdogError> {
    pretty_env_logger::init();
    info!("Starting hotdog version {}", env!["CARGO_PKG_VERSION"]);

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

/// This function pins the running smol threads to cores to reduce the potential context switching
/// and cache misses when processing data as threads are spread around. This _may_ be a premature
/// optimization.
fn pin_cores() {
    use affinity::*;
    let cores: Vec<usize> = match std::env::var("SMOL_THREADS") {
        Ok(count) => {
            let count: usize = count.parse().expect("Failed to parse the SMOL_THREADS");
            debug!("Requested {count} threads for smol");
            let core_count = get_core_num();
            debug!("Counting {core_count} cores");
            // If we've been asked to use a _lot_ of threads, forget core affinity
            if count > core_count {
                return;
            }
            // If threads are equal to cores, pin each one by one
            if core_count == count {
                (0..get_core_num()).step_by(1).collect()
            }
            // Otherwise step by two to avoid pinning to hyperthreaded "cores" and get physical
            // CPUs if possible
            else {
                (0..get_core_num()).step_by(2).collect()
            }
        }
        Err(_) => (0..get_core_num()).step_by(2).collect(),
    };
    info!("Binding thread to cores : {:?}", &cores);

    set_thread_affinity(&cores).unwrap();
    info!(
        "Current thread affinity : {:?}",
        get_thread_affinity().unwrap()
    );
}

/// Main asynchronous runloop
async fn run() -> Result<(), errors::HotdogError> {
    pin_cores();
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
    let metrics = Statsd::send_to(&settings.global.metrics.statsd)
        .expect("Failed to create Statsd recorder")
        .named("hotdog")
        .metrics();

    let input = InputQueueScope::wrap(metrics, 1_000);

    if let Some(st) = &settings.global.status {
        let _task = smol::spawn(status::status_server(format!("{}:{}", st.address, st.port)));
    }

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
        stats: input,
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
