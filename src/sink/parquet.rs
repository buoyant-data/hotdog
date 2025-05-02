//!
//! The Parquet module contains the parquet sink which is mostly intended to be used with S3
//! storage backends
//!

use super::{Message, Sink};

use arrow_json::reader::{ReaderBuilder, infer_json_schema};
use async_channel::{Receiver, Sender, unbounded};
use async_compat::Compat;
use dipstick::InputQueueScope;
use object_store::ObjectStore;
use parquet::arrow::async_writer::{AsyncArrowWriter, ParquetObjectWriter};
use smol::stream::StreamExt;
use tracing::log::*;
use tracing::{Level, span};
use url::Url;
use uuid::Uuid;

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::schema::into_arrow_schema;

/// Alias for convenience in refering to reference counted [ObjectStore]
type ObjectStoreRef = Arc<dyn ObjectStore>;

/// Parquet sink which handles creating parquet files from buffers and writing them into the
/// storage layer
#[derive(Clone)]
pub struct Parquet {
    /// Configuration from the hotdog.yml
    config: Config,
    /// Underlying object store
    store: ObjectStoreRef,
    /// Schemas that can be used, keyed by the output "topic" identified
    schemas: HashMap<String, arrow_schema::SchemaRef>,
    /// Receiver side of the channel for this sink
    rx: Receiver<Message>,
    /// Producer side of the channel for this sink
    tx: Sender<Message>,
}

#[async_trait::async_trait]
impl Sink for Parquet {
    type Config = Config;

    fn new(
        config: Self::Config,
        schemas: &[crate::settings::Schema],
        _stats: InputQueueScope,
    ) -> Self {
        let (tx, rx) = unbounded();
        // [object_store] largely expects environment variables to be all lowercased for
        // consideration as options
        let opts: HashMap<String, String> =
            HashMap::from_iter(std::env::vars().map(|(k, v)| (k.to_ascii_lowercase(), v)));
        let (store, _path) = object_store::parse_url_opts(&config.url, opts)
            .expect("Failed to parse the Parquet sink URL");
        trace!("Converting schemas: {schemas:?}");

        let schemas = HashMap::from_iter(schemas.iter().map(|s| {
            (
                s.topic.clone(),
                Arc::new(into_arrow_schema(&s.fields).expect("Failed to convert a schema!")),
            )
        }));
        debug!("Loading sink with schemas: {schemas:?}");
        let store = Arc::new(store);
        Parquet {
            config,
            tx,
            rx,
            store,
            schemas,
        }
    }

    fn get_sender(&self) -> Sender<Message> {
        self.tx.clone()
    }

    async fn runloop(&self) {
        // Converting this at the beginning of the function to ensure that it doesn't need to be
        // done in the loop and if the configuration is invalid then the sink fails as early as
        // possible
        let flush_ms: u128 = self
            .config
            .flush_ms
            .try_into()
            .expect("Failed to convert the flush_ms to a 128 bit integer");

        let interval = Duration::from_millis(
            self.config
                .flush_ms
                .try_into()
                .expect("Failed to convert to u64"),
        );
        let timer_tx = self.tx.clone();

        let _ = smol::spawn(async move {
            info!("The flush timer has been set up for {interval:?}");
            let mut timer = smol::Timer::interval(interval);
            while timer.next().await.is_some() {
                debug!("Timer has fired, issuing a flush");
                if let Err(e) = timer_tx.send(Message::flush()).await {
                    error!("Failed to trigger the flush timer in the parquet sink: {e:?}");
                }
            }
        })
        .detach();

        debug!("Entering the Parquet sink runloop");
        smol::block_on(Compat::new(async {
            info!("Listing the bucket as a sanity check: {}", self.config.url);
            let mut list_stream = self.store.list(None);

            // Print a line about each object
            if let Some(meta) = list_stream.next().await.transpose().unwrap() {
                debug!("Name: {}, size: {}", meta.location, meta.size);
            }
            debug!("Finished listing the bucket");
        }));

        let mut buffer: HashMap<String, Vec<u8>> = HashMap::default();
        let mut bufsizes: HashMap<String, usize> = HashMap::default();
        let mut since_last_flush = Instant::now();

        loop {
            if let Ok(msg) = self.rx.recv().await {
                smol::future::yield_now().await;

                match msg {
                    Message::Data {
                        destination,
                        payload,
                    } => {
                        let _span = span!(Level::TRACE, "Parquet sink recv");
                        debug!("Buffering this message for Parquet output: {destination}");

                        if !buffer.contains_key(&destination) {
                            buffer.insert(destination.clone(), vec![]);
                            bufsizes.insert(destination.clone(), 0);
                        }

                        if let Some(queue) = buffer.get_mut(&destination) {
                            let bufsize = bufsizes
                                .get_mut(&destination)
                                .expect("Failed to get the bufsizes somehow");
                            (*bufsize) += payload.len();
                            debug!(
                                "enqueing into `{}` (bytes: {bufsize}): {:?}",
                                &destination, &payload
                            );
                            queue.extend(payload.as_bytes());
                            queue.extend("\n".as_bytes());

                            if let Some(max_buffer) = &self.config.buffer {
                                if (*bufsize) >= *max_buffer {
                                    debug!(
                                        "Reached the threshold to flush bytes for `{}`",
                                        &destination
                                    );
                                    let _ = self.tx.send(Message::flush()).await;
                                }
                            }
                        }
                    }
                    Message::Flush { should_exit } => {
                        debug!("Parquet sink has been told to flush");

                        for (destination, buf) in buffer.drain() {
                            let _flush_span = span!(Level::INFO, "Parquet flush for", destination);

                            if let Some(schema) = self.schemas.get(&destination) {
                                flush_to_parquet(
                                    self.store.clone(),
                                    schema.clone(),
                                    &destination,
                                    &buf,
                                )
                                .await;
                            } else {
                                info!(
                                    "Did not have a schema, so will try inferring one for {destination}!"
                                );
                                let payload = buf.as_slice();
                                let payload = &payload[0..buf
                                    .iter()
                                    .position(|b| *b == b'\n')
                                    .expect("Failed to find a newline for schema inference")];

                                // NOTE: this is poorly tested and needs some unit test
                                // coverage
                                let payload = String::from_utf8_lossy(payload);
                                debug!(
                                    "Using the following payload to infer the schema for {destination}: {payload}"
                                );
                                // Use the most recent payload for the inference
                                let mut cursor: Cursor<&str> = Cursor::new(&payload);
                                match infer_json_schema(&mut cursor, None) {
                                    Ok((inferred_schema, _read)) => {
                                        flush_to_parquet(
                                            self.store.clone(),
                                            Arc::new(inferred_schema),
                                            &destination,
                                            &buf,
                                        )
                                        .await;
                                    }
                                    Err(err) => {
                                        error!(
                                            "Failed to infer a JSON schema from the payload for {destination}! {err:?}"
                                        );
                                        error!(
                                            "I cannot do anything but throw away the data v_v: {payload}"
                                        );
                                    }
                                }
                            }
                        }
                        since_last_flush = Instant::now();

                        if should_exit {
                            debug!("Supposed to exit from the sink!");
                            return;
                        }
                    }
                }
            }
        }
    }
}

/// Write the given buffer to a new `.parquet` file in the [ObjectStore]
async fn flush_to_parquet(
    store: ObjectStoreRef,
    schema: Arc<arrow_schema::Schema>,
    destination: &str,
    buffer: &[u8],
) {
    if buffer.is_empty() {
        warn!("Attempted to flush_to_parquet with an empty buffer");
        return;
    }
    info!(
        "Flushing buffer with {} bytes to {destination}",
        buffer.len()
    );
    trace!("Using the schema to build parquet file: {schema:?}");

    let mut decoder = ReaderBuilder::new(schema.clone())
        .build_decoder()
        .expect("Failed to build the JSON decoder");
    let output =
        object_store::path::Path::from(format!("{destination}/{}.parquet", Uuid::new_v4()));
    let object_writer = ParquetObjectWriter::new(store.clone(), output.clone());
    let mut writer = AsyncArrowWriter::try_new(object_writer, schema.clone(), None)
        .expect("Failed to build AsyncArrowWriter");

    let total_bytes = buffer.len();
    let mut read_bytes = 0;

    Compat::new(async {
        loop {
            let decoded = decoder
                .decode(&buffer[read_bytes..])
                .expect("Failed to deserialize bytes");
            debug!("Decoded {decoded} bytes");
            read_bytes += decoded;

            if let Some(batch) = decoder
                .flush()
                .expect("Failed to flush bytes to a RecordBatch")
            {
                debug!("Wrote a batch");
                writer.write(&batch).await.expect("Failed to write a batch");
            }

            if read_bytes >= total_bytes {
                break;
            }
        }
        let file_result = writer.close().await.expect("Failed to close the writer");
        info!("Flushed {} rows to storage", file_result.num_rows);
    })
    .await;
}

/// Configuration for [Parquet] sink
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct Config {
    #[serde(default = "parquet_url_default")]
    /// Expected to be an S3 compatible URL
    pub url: Url,
    /// Minimum number of bytes to buffer into each parquet file
    pub buffer: Option<usize>,
    /// Duration in milliseconds before a flush to storage should happen
    #[serde(default = "parquet_flush_default")]
    pub flush_ms: usize,
}

/// Retrieves a URL from the environment for parquet if no [Url] has been specified
fn parquet_url_default() -> Url {
    Url::parse(&std::env::var("S3_OUTPUT_URL").expect(
        "There is no url: defined for the parquet sink and no S3_OUTPUT_URL in the environment!",
    ))
    .expect("The S3_OUTPUT_URL could not be parsed as a valid URL")
}

/// Default milliseconds before a Parquet sink flush
fn parquet_flush_default() -> usize {
    1000 * 60
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deser_config() {
        let conf = r#"
---
url: 's3://bucket'
        "#;
        let parquet: Config = serde_yaml::from_str(conf).expect("Failed to deserialize");
        assert_eq!(parquet.flush_ms, parquet_flush_default());
        assert_eq!(
            parquet.url,
            Url::parse("s3://bucket").expect("Failed to parse a basic URL")
        );
    }

    #[test]
    fn test_defaults() {
        assert!(0 < parquet_flush_default());
    }
}
