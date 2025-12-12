use std::convert::TryFrom;

use anyhow::{Context as AnyhowContext, Result};
use futures::TryFutureExt;
use opendal::{Operator, layers::LoggingLayer};
use restate_sdk::{context::Context, errors::HandlerError, prelude::*, serde::Json};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::common::{
    self, DownloadResponse, RequestOptions, filename_from_response, process_download, send_request,
    terminal,
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
    pub output: OutputOptions,
}

fn example_download_request() -> DownloadRequest {
    DownloadRequest {
        url: Url::parse(
            "https://download.blender.org/peach/bigbuckbunny_movies/big_buck_bunny_1080p_h264.mov",
        )
        .unwrap(),
        request: None,
        output: OutputOptions {
            url: Url::parse("s3://bucket").unwrap(),
            common: common::OutputOptions {
                set_content_type: false,
                content_type: None,
            },
        },
    }
}

/// Output options for a downloaded file
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct OutputOptions {
    /// Storage URL to save the file to
    pub url: Url,
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
}

impl DownloaderImpl {
    pub fn new(client: reqwest::Client) -> Self {
        Self { client }
    }

    async fn _download(&self, request: DownloadRequest) -> Result<u64, HandlerError> {
        let response = send_request(&self.client, request.url, request.request).await?;

        let mut output_url = request.output.url.clone();

        let filename = if output_url.path().is_empty() || output_url.path().ends_with('/') {
            // Path is empty or is a directory, get filename from response
            filename_from_response(&response)?
        } else {
            // Extract filename from URL path and remove it from the URL
            let filename = output_url
                .path_segments()
                .and_then(|mut s| s.next_back())
                .filter(|s| !s.is_empty())
                .map(String::from)
                .unwrap_or_else(|| "download".into());

            output_url
                .path_segments_mut()
                .map_err(|_| anyhow::anyhow!("Cannot modify URL path"))
                .map_err(terminal)?
                .pop();

            filename
        };

        let operator = Operator::from_uri(output_url.as_str())
            .context("Failed to create operator from config")
            .map_err(terminal)?
            .layer(LoggingLayer::default());

        process_download(&operator, response, filename, Some(request.output.common)).await
    }
}

impl Downloader for DownloaderImpl {
    async fn download(
        &self,
        ctx: Context<'_>,
        request: Json<DownloadRequest>,
    ) -> Result<Json<DownloadResponse>, HandlerError> {
        let request = request.into_inner();

        let size = ctx
            .run(|| self._download(request).map_err(HandlerError::from))
            .await?;

        Ok(Json(DownloadResponse { size }))
    }
}
