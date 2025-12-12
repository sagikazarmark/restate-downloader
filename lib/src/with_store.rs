use std::convert::TryFrom;

use anyhow::Result;
use opendal::Operator;
use reqwest::Response;
use restate_sdk::{context::Context, errors::HandlerError, prelude::*, serde::Json};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use typed_path::UnixPathBuf;
use url::Url;

use crate::common::{
    self, DownloadResponse, RequestOptions, filename_from_response, process_download, send_request,
};

/// Request to download a file from URL and save it to storage
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(example = example_download_request())]
pub struct DownloadRequest {
    /// URL to download from
    pub url: Url,
    /// Request options
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request: Option<RequestOptions>,
    /// Output options
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<OutputOptions>,
}

fn example_download_request() -> DownloadRequest {
    DownloadRequest {
        url: Url::parse("https://example.com/file.pdf").unwrap(),
        request: None,
        output: None,
    }
}

/// Output options for a downloaded file
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OutputOptions {
    /// Path to save the file to
    #[schemars(length(min = 1))]
    pub path: Option<PosixPath>,
    #[serde(flatten)]
    common: common::OutputOptions,
}

#[restate_sdk::service]
pub trait Downloader {
    async fn download(
        request: Json<DownloadRequest>,
    ) -> Result<Json<DownloadResponse>, HandlerError>;
}

pub struct DownloaderImpl {
    client: reqwest::Client,
    operator: Operator,
}

impl DownloaderImpl {
    pub fn new(client: reqwest::Client, operator: Operator) -> Self {
        Self { client, operator }
    }

    async fn _download(&self, request: DownloadRequest) -> Result<u64, HandlerError> {
        let response = send_request(&self.client, request.url, request.request).await?;

        let filepath = resolve_filepath(request.output.clone().and_then(|o| o.path), &response)?;

        process_download(
            &self.operator,
            response,
            filepath,
            request.output.map(|o| o.common),
        )
        .await
    }
}

impl Downloader for DownloaderImpl {
    async fn download(
        &self,
        ctx: Context<'_>,
        request: Json<DownloadRequest>,
    ) -> Result<Json<DownloadResponse>, HandlerError> {
        let request = request.into_inner();

        let size = ctx.run(|| self._download(request)).await?;

        Ok(Json(DownloadResponse { size }))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
#[schemars(transparent)]
pub struct PosixPath(String);

impl Into<UnixPathBuf> for PosixPath {
    fn into(self) -> UnixPathBuf {
        self.as_unix_path()
    }
}

impl PosixPath {
    pub fn as_unix_path(&self) -> UnixPathBuf {
        UnixPathBuf::from(&self.0)
    }
}

fn resolve_filepath(path: Option<PosixPath>, response: &Response) -> Result<String> {
    let Some(path) = path else {
        return filename_from_response(response);
    };

    let unix_path = path.as_unix_path();
    let has_trailing_slash = unix_path.to_string().ends_with('/');
    let normalized = unix_path.normalize();

    if has_trailing_slash || normalized.to_string().is_empty() {
        let filename = filename_from_response(response)?;

        Ok(normalized.join(filename).to_string())
    } else {
        Ok(normalized.to_string())
    }
}
