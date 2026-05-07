//!
//! The Parquet module contains the parquet sink which is mostly intended to be used with S3
//! storage backends
//!

use super::{Message, Sink};

use arrow_array::RecordBatch;
use arrow_json::reader::{ReaderBuilder, infer_json_schema};
use arrow_schema::{ArrowError, SchemaRef};
use dipstick::InputQueueScope;
use futures::StreamExt;
use object_store::ObjectStore;
use parquet::arrow::async_writer::{AsyncArrowWriter, ParquetObjectWriter};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
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
pub struct Parquet {
    /// Configuration from the hotdog.yml
    config: Config,
    /// Underlying object store
    store: ObjectStoreRef,
    /// Schemas that can be used, keyed by the output "topic" identified
    schemas: HashMap<String, arrow_schema::SchemaRef>,
    /// Receiver side of the channel for this sink
    rx: UnboundedReceiver<Message>,
    /// Producer side of the channel for this sink
    tx: UnboundedSender<Message>,
}

#[async_trait::async_trait]
impl Sink for Parquet {
    type Config = Config;

    fn new(
        config: Self::Config,
        schemas: &[crate::settings::Schema],
        _stats: InputQueueScope,
    ) -> Self {
        let (tx, rx) = unbounded_channel();
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

    fn get_sender(&self) -> UnboundedSender<Message> {
        self.tx.clone()
    }

    async fn runloop(&mut self) {
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

        tokio::spawn(async move {
            info!("The flush timer has been set up for {interval:?}");
            let mut timer = tokio::time::interval(interval);
            // Skip the immediate first tick that tokio's interval emits.
            timer.tick().await;
            loop {
                timer.tick().await;
                debug!("Timer has fired, issuing a flush");
                if let Err(e) = timer_tx.send(Message::flush()) {
                    error!("Failed to trigger the flush timer in the parquet sink: {e:?}");
                }
            }
        });

        debug!("Entering the Parquet sink runloop");
        info!("Listing the bucket as a sanity check: {}", self.config.url);
        let mut list_stream = self.store.list(None);
        if let Some(meta) = list_stream.next().await.transpose().unwrap() {
            debug!("Name: {}, size: {}", meta.location, meta.size);
        }
        drop(list_stream);
        debug!("Finished listing the bucket");

        let mut buffer: HashMap<String, Vec<u8>> = HashMap::default();
        let mut bufsizes: HashMap<String, usize> = HashMap::default();
        let mut since_last_flush = Instant::now();

        loop {
            if let Some(msg) = self.rx.recv().await {
                tokio::task::yield_now().await;

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
                                    let _ = self.tx.send(Message::flush());
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

    let batches = match decode_bulk(buffer, schema.clone()) {
        Ok(batches) => batches,
        Err(err) => {
            warn!(
                "Bulk JSON decode failed for {destination} ({err}); falling back to per-line decode to salvage valid rows"
            );
            let (batches, skipped) = decode_per_line(buffer, schema.clone());
            if skipped > 0 {
                warn!("Skipped {skipped} unparseable record(s) for {destination}");
            }
            batches
        }
    };

    if batches.is_empty() {
        warn!("No valid records to write for {destination}");
        return;
    }

    let output =
        object_store::path::Path::from(format!("{destination}/{}.parquet", Uuid::new_v4()));
    let object_writer = ParquetObjectWriter::new(store.clone(), output.clone());
    let mut writer = AsyncArrowWriter::try_new(object_writer, schema.clone(), None)
        .expect("Failed to build AsyncArrowWriter");

    for batch in batches {
        writer.write(&batch).await.expect("Failed to write a batch");
    }
    let file_result = writer.close().await.expect("Failed to close the writer");
    info!("Flushed {} rows to storage", file_result.num_rows);
}

/// Decode the entire `buffer` of newline-delimited JSON into batches in one shot.
///
/// Returns `Err` if any row in the buffer fails to parse or coerce to the schema; the caller
/// is expected to fall back to [`decode_per_line`] to salvage the good rows.
fn decode_bulk(buffer: &[u8], schema: SchemaRef) -> Result<Vec<RecordBatch>, ArrowError> {
    let mut decoder = ReaderBuilder::new(schema).build_decoder()?;
    let mut batches = Vec::new();
    let total_bytes = buffer.len();
    let mut read_bytes = 0;

    while read_bytes < total_bytes {
        let decoded = decoder.decode(&buffer[read_bytes..])?;
        if decoded == 0 {
            break;
        }
        read_bytes += decoded;
        if let Some(batch) = decoder.flush()? {
            batches.push(batch);
        }
    }
    if let Some(batch) = decoder.flush()? {
        batches.push(batch);
    }
    Ok(batches)
}

/// Decode `buffer` line by line with a fresh decoder per line so that one bad record only
/// loses itself rather than the whole batch. Returns the successfully decoded batches and a
/// count of skipped records.
fn decode_per_line(buffer: &[u8], schema: SchemaRef) -> (Vec<RecordBatch>, usize) {
    let mut batches = Vec::new();
    let mut skipped: usize = 0;

    for line in buffer.split(|b| *b == b'\n') {
        if line.is_empty() {
            continue;
        }
        match decode_one_line(line, schema.clone()) {
            Ok(Some(batch)) => batches.push(batch),
            Ok(None) => {}
            Err(err) => {
                let preview = String::from_utf8_lossy(line);
                let truncated: String = preview.chars().take(256).collect();
                warn!("Skipping unparseable record ({err}): {truncated:?}");
                skipped += 1;
            }
        }
    }

    (batches, skipped)
}

fn decode_one_line(line: &[u8], schema: SchemaRef) -> Result<Option<RecordBatch>, ArrowError> {
    let mut decoder = ReaderBuilder::new(schema).build_decoder()?;
    let mut framed = Vec::with_capacity(line.len() + 1);
    framed.extend_from_slice(line);
    framed.push(b'\n');
    let mut read = 0;
    while read < framed.len() {
        let n = decoder.decode(&framed[read..])?;
        if n == 0 {
            break;
        }
        read += n;
    }
    decoder.flush()
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

    fn user_id_schema() -> SchemaRef {
        Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "user_id",
            arrow_schema::DataType::Int64,
            true,
        )]))
    }

    #[test]
    fn test_decode_bulk_fails_on_bad_row() {
        let buf = b"{\"user_id\": 1}\n{\"user_id\": \"\"}\n{\"user_id\": 3}\n";
        let result = decode_bulk(buf, user_id_schema());
        assert!(
            result.is_err(),
            "expected bulk decode to fail on a row with empty-string user_id"
        );
    }

    #[test]
    fn test_decode_per_line_skips_bad_row() {
        let buf = b"{\"user_id\": 1}\n{\"user_id\": \"\"}\n{\"user_id\": 3}\n";
        let (batches, skipped) = decode_per_line(buf, user_id_schema());
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2, "expected the two valid rows to survive");
        assert_eq!(skipped, 1, "expected exactly one row to be skipped");
    }
}
