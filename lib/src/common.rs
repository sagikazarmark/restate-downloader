use std::{collections::HashMap, time::Duration};

use anyhow::{Context as _, Result};
use content_disposition::parse_content_disposition;
use futures::{Stream, StreamExt as _};
use opendal::{Operator, Writer};
use reqwest::{
    Response,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use restate_sdk::errors::{HandlerError, TerminalError};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RequestOptions {
    /// Headers to send with the request
    #[serde(default)]
    pub headers: HashMap<String, String>,
    /// Timeout for download (accepted values are human-readable duration strings, eg. "10m", "1h 30m", etc)
    #[serde(default, with = "humantime_serde")]
    #[schemars(with = "Option<String>")]
    pub timeout: Option<Duration>,
}

impl TryFrom<RequestOptions> for HeaderMap {
    type Error = anyhow::Error;

    fn try_from(config: RequestOptions) -> Result<Self, Self::Error> {
        let mut map = HeaderMap::new();

        for (key, value) in config.headers {
            let name = HeaderName::from_bytes(key.as_bytes())?;
            let value = HeaderValue::from_str(&value)?;

            map.insert(name, value);
        }

        Ok(map)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OutputOptions {
    /// Set the content type of the downloaded file
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub set_content_type: bool,
    /// Content type override for the downloaded file (falls back to the content type of the downloaded file)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
}

/// Response from the download operation
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DownloadResponse {
    pub size: u64,
}

pub(crate) fn create_request(
    client: &reqwest::Client,
    url: Url,
    options: Option<RequestOptions>,
) -> Result<reqwest::RequestBuilder> {
    let mut request = client.get(url);

    if let Some(options) = options {
        let timeout = options.timeout;
        request = request.headers(options.try_into()?);

        if let Some(timeout) = timeout {
            request = request.timeout(timeout);
        }
    }

    Ok(request)
}

pub(crate) async fn send_request(
    client: &reqwest::Client,
    url: Url,
    options: Option<RequestOptions>,
) -> Result<reqwest::Response, HandlerError> {
    create_request(client, url, options)
        .map_err(terminal)?
        .send()
        .await?
        .error_for_status()
        .map_err(http_error)
}

pub(crate) fn filename_from_response(response: &Response) -> Result<String> {
    filename_from_headers(response.headers())
        .or_else(|| filename_from_url(response.url()))
        .context("Failed to determine filename from the response")
}

fn filename_from_url(url: &Url) -> Option<String> {
    url.path_segments()
        .and_then(|mut s| s.next_back())
        .map(String::from)
}

fn filename_from_headers(headers: &HeaderMap) -> Option<String> {
    headers
        .get("content-disposition")
        .and_then(|cd| cd.to_str().ok())
        .and_then(|cd| parse_content_disposition(cd).filename_full())
}

pub(crate) async fn create_writer(
    operator: &Operator,
    headers: &HeaderMap,
    filepath: String,
    output: Option<OutputOptions>,
) -> Result<Writer, anyhow::Error> {
    let mut writer_builder = operator.writer_with(filepath.as_str());

    if let Some(output) = output
        && output.set_content_type
    {
        let content_type = output.content_type.or_else(|| {
            headers
                .get("content-type")
                .and_then(|ct| ct.to_str().ok())
                .map(String::from)
        });

        if let Some(ct) = content_type {
            writer_builder = writer_builder.content_type(&ct);
        }
    }

    writer_builder
        .await
        .context("Failed to create storage writer")
}

pub(crate) async fn stream_file<S>(
    mut stream: S,
    mut writer: Writer,
) -> std::result::Result<u64, anyhow::Error>
where
    S: Stream<Item = std::result::Result<bytes::Bytes, reqwest::Error>> + Unpin,
{
    let mut size = 0u64;

    // Stream data directly from HTTP response to storage
    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result.with_context(|| "Failed to read chunk from HTTP response")?;

        size += chunk.len() as u64;

        writer
            .write(chunk)
            .await
            .context("Failed to write chunk to storage")?;
    }

    // Close the writer to finalize the upload
    writer
        .close()
        .await
        .context("Failed to finalize storage upload")?;

    Ok(size)
}

pub async fn process_download(
    operator: &Operator,
    response: reqwest::Response,
    filepath: String,
    output: Option<OutputOptions>,
) -> Result<u64, HandlerError> {
    let writer = create_writer(operator, response.headers(), filepath, output).await?;

    let stream = response.bytes_stream();

    let size = stream_file(stream, writer)
        .await
        .context("Failed to stream file to storage")?;

    Ok(size)
}

pub fn terminal_error(err: anyhow::Error) -> TerminalError {
    TerminalError::new(err.to_string())
}
/// Convert an error to a terminal HandlerError
pub fn terminal<E: std::fmt::Display>(e: E) -> HandlerError {
    TerminalError::new(e.to_string()).into()
}

pub fn http_error(e: reqwest::Error) -> HandlerError {
    match e.status() {
        Some(status) => {
            let err = anyhow::Error::new(e)
                .context(format!("HTTP request failed with status: {}", status));
            if status.is_client_error() {
                terminal(err)
            } else {
                err.into()
            }
        }
        None => e.into(),
    }
}
