use std::convert::TryFrom;

use anyhow::{Context as AnyhowContext, Result};
use opendal::{Operator, layers::LoggingLayer};
use restate_sdk::prelude::*;
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
    #[serde(rename = "request", skip_serializing_if = "Option::is_none")]
    pub request_options: Option<RequestOptions>,
    /// Output options
    pub output: OutputOptions,
}

fn example_download_request() -> DownloadRequest {
    DownloadRequest {
        url: Url::parse(
            "https://download.blender.org/peach/bigbuckbunny_movies/big_buck_bunny_1080p_h264.mov",
        )
        .unwrap(),
        request_options: None,
        output: OutputOptions {
            uri: Url::parse("s3://bucket").unwrap(),
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
    /// Storage URI to save the file to
    pub uri: Url,
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

    async fn _download(&self, request: DownloadRequest) -> Result<DownloadResponse, HandlerError> {
        let response = send_request(&self.client, request.url, request.request_options).await?;

        let (uri, path) = resolve_uri_and_path(request.output.uri, &response)?;

        let operator = Operator::from_uri(uri.as_str())
            .context("Failed to create operator from config")
            .map_err(terminal)?
            .layer(LoggingLayer::default());

        let size = process_download(
            &operator,
            response,
            path.as_str(),
            Some(request.output.common),
        )
        .await?;

        Ok(DownloadResponse { path, size })
    }
}

fn resolve_uri_and_path(
    mut uri: Url,
    response: &reqwest::Response,
) -> Result<(Url, String), HandlerError> {
    let path = if uri.path().is_empty() || uri.path().ends_with('/') {
        filename_from_response(response)?
    } else {
        let path = uri
            .path_segments()
            .and_then(|mut s| s.next_back())
            .filter(|s| !s.is_empty())
            .map(String::from)
            .unwrap_or_else(|| "download".into());

        uri.path_segments_mut()
            .map_err(|_| anyhow::anyhow!("Cannot modify URL path"))
            .map_err(terminal)?
            .pop();

        path
    };

    Ok((uri, path))
}

impl Downloader for DownloaderImpl {
    async fn download(
        &self,
        ctx: Context<'_>,
        request: Json<DownloadRequest>,
    ) -> Result<Json<DownloadResponse>, HandlerError> {
        Ok(ctx
            .run(async || self._download(request.into_inner()).await.map(Json))
            .await?)
    }
}
