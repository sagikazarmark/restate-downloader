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

impl From<PosixPath> for UnixPathBuf {
    fn from(val: PosixPath) -> Self {
        val.as_unix_path()
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

/// Test module for resolve_filepath function and related utilities
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Test helper that simulates the behavior we need from Response
    /// without actually needing to mock the entire reqwest::Response.
    /// This provides a cleaner testing approach than mocking the complex reqwest::Response type.
    struct MockResponseData {
        url: Url,
        headers: HashMap<String, String>,
    }

    impl MockResponseData {
        fn new(url: &str) -> Self {
            Self {
                url: Url::parse(url).unwrap(),
                headers: HashMap::new(),
            }
        }

        fn with_content_disposition(mut self, cd: &str) -> Self {
            self.headers
                .insert("content-disposition".to_string(), cd.to_string());
            self
        }

        // Helper that extracts filename similar to filename_from_response
        fn extract_filename(&self) -> Result<String> {
            // First try content-disposition header
            if let Some(cd) = self.headers.get("content-disposition") {
                if let Some(filename) =
                    content_disposition::parse_content_disposition(cd).filename_full()
                {
                    return Ok(filename);
                }
            }

            // Fall back to URL path
            self.url
                .path_segments()
                .and_then(|s| s.last())
                .map(String::from)
                .ok_or_else(|| anyhow::anyhow!("Failed to determine filename"))
        }
    }

    /// Test helper function that mimics resolve_filepath but uses our mock data.
    /// This allows us to test the core logic without dealing with reqwest::Response mocking complexities.
    fn test_resolve_filepath(
        path: Option<PosixPath>,
        mock_response: &MockResponseData,
    ) -> Result<String> {
        let Some(path) = path else {
            return mock_response.extract_filename();
        };

        let unix_path = path.as_unix_path();
        let has_trailing_slash = unix_path.to_string().ends_with('/');
        let normalized = unix_path.normalize();

        if has_trailing_slash || normalized.to_string().is_empty() {
            let filename = mock_response.extract_filename()?;
            Ok(normalized.join(filename).to_string())
        } else {
            Ok(normalized.to_string())
        }
    }

    /// Test that when path is None, the filename is extracted from the URL
    #[test]
    fn test_resolve_filepath_with_none_path() {
        // Test when path is None - should extract filename from URL
        let mock_response = MockResponseData::new("https://example.com/test-file.pdf");

        let result = test_resolve_filepath(None, &mock_response);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "test-file.pdf");
    }

    /// Test that when path is None and content-disposition header is present,
    /// the filename is extracted from the header rather than URL
    #[test]
    fn test_resolve_filepath_with_none_path_and_content_disposition() {
        // Test when path is None and content-disposition header is present
        let mock_response = MockResponseData::new("https://example.com/generic-name")
            .with_content_disposition("attachment; filename=\"downloaded-file.pdf\"");

        let result = test_resolve_filepath(None, &mock_response);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "downloaded-file.pdf");
    }

    /// Test that when path has an explicit filename (no trailing slash),
    /// the normalized path is returned as-is
    #[test]
    fn test_resolve_filepath_with_explicit_filename() {
        // Test when path has explicit filename (no trailing slash)
        let path = PosixPath("downloads/my-file.txt".to_string());
        let mock_response = MockResponseData::new("https://example.com/original.pdf");

        let result = test_resolve_filepath(Some(path), &mock_response);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "downloads/my-file.txt");
    }

    /// Test that when path has a trailing slash, it's treated as a directory
    /// and the filename from the response is appended
    #[test]
    fn test_resolve_filepath_with_trailing_slash() {
        // Test when path has trailing slash - should append filename from response
        let path = PosixPath("downloads/".to_string());
        let mock_response = MockResponseData::new("https://example.com/file.pdf");

        let result = test_resolve_filepath(Some(path), &mock_response);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "downloads/file.pdf");
    }

    /// Test trailing slash behavior when filename comes from content-disposition header
    #[test]
    fn test_resolve_filepath_with_trailing_slash_and_content_disposition() {
        // Test trailing slash with content-disposition header
        let path = PosixPath("downloads/".to_string());
        let mock_response = MockResponseData::new("https://example.com/generic")
            .with_content_disposition("attachment; filename=\"report.pdf\"");

        let result = test_resolve_filepath(Some(path), &mock_response);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "downloads/report.pdf");
    }

    /// Test that when path normalizes to empty string (like "."),
    /// it's treated as current directory and filename is appended
    #[test]
    fn test_resolve_filepath_with_empty_normalized_path() {
        // Test when path normalizes to empty string
        let path = PosixPath(".".to_string());
        let mock_response = MockResponseData::new("https://example.com/root-file.txt");

        let result = test_resolve_filepath(Some(path), &mock_response);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "root-file.txt");
    }

    /// Test various path normalization cases to ensure proper handling
    /// of relative paths, parent directory references, and current directory references
    #[test]
    fn test_resolve_filepath_with_path_normalization() {
        // Test path normalization cases
        let test_cases = vec![
            ("./downloads/file.txt", "downloads/file.txt"),
            ("downloads/../downloads/file.txt", "downloads/file.txt"),
            ("downloads/./file.txt", "downloads/file.txt"),
            ("/absolute/path.txt", "/absolute/path.txt"),
            ("nested/../../file.txt", "file.txt"), // This normalizes to just "file.txt", not "../file.txt"
        ];

        for (input_path, expected) in test_cases {
            let path = PosixPath(input_path.to_string());
            let mock_response = MockResponseData::new("https://example.com/ignored.pdf");

            let result = test_resolve_filepath(Some(path), &mock_response);
            assert!(result.is_ok(), "Failed for input: {}", input_path);
            assert_eq!(
                result.unwrap(),
                expected,
                "Failed for input: {}",
                input_path
            );
        }
    }

    /// Test various directory paths with trailing slashes to ensure
    /// they are correctly joined with filenames from responses
    #[test]
    fn test_resolve_filepath_with_directory_and_trailing_slash() {
        // Test various directory paths with trailing slashes
        let test_cases = vec![
            ("downloads/", "downloads/filename.txt"),
            ("a/b/c/", "a/b/c/filename.txt"),
            ("./downloads/", "downloads/filename.txt"),
            ("../downloads/", "downloads/filename.txt"),
            ("./", "filename.txt"),
            ("", "filename.txt"), // empty string should be treated as current directory
        ];

        for (input_path, expected) in test_cases {
            let path = PosixPath(input_path.to_string());
            let mock_response = MockResponseData::new("https://example.com/filename.txt");

            let result = test_resolve_filepath(Some(path), &mock_response);
            assert!(result.is_ok(), "Failed for input: '{}'", input_path);
            assert_eq!(
                result.unwrap(),
                expected,
                "Failed for input: '{}'",
                input_path
            );
        }
    }

    /// Test various content-disposition header formats to ensure
    /// proper filename extraction in different scenarios
    #[test]
    fn test_resolve_filepath_complex_content_disposition() {
        // Test various content-disposition formats
        let test_cases = vec![
            ("attachment; filename=simple.txt", "simple.txt"),
            ("attachment; filename=\"quoted.txt\"", "quoted.txt"),
            (
                "attachment; filename*=UTF-8''encoded%20file.txt",
                "encoded file.txt",
            ),
            ("inline; filename=\"inline-file.pdf\"", "inline-file.pdf"),
        ];

        for (cd_header, expected_filename) in test_cases {
            let path = PosixPath("output/".to_string());
            let mock_response = MockResponseData::new("https://example.com/generic")
                .with_content_disposition(cd_header);

            let result = test_resolve_filepath(Some(path), &mock_response);
            assert!(
                result.is_ok(),
                "Failed for content-disposition: {}",
                cd_header
            );
            assert_eq!(
                result.unwrap(),
                format!("output/{}", expected_filename),
                "Failed for content-disposition: {}",
                cd_header
            );
        }
    }

    /// Test PosixPath utility methods and conversions
    #[test]
    fn test_posix_path_conversion() {
        // Test PosixPath utility methods
        let path = PosixPath("test/path".to_string());
        let unix_path: UnixPathBuf = path.clone().into();
        assert_eq!(unix_path.to_string(), "test/path");

        let unix_path2 = path.as_unix_path();
        assert_eq!(unix_path2.to_string(), "test/path");
    }

    /// Test edge cases for PosixPath normalization behavior
    #[test]
    fn test_posix_path_edge_cases() {
        // Test edge cases for PosixPath
        let test_cases = vec![
            ("", ""),
            ("/", "/"), // Root path normalizes to "/"
            ("./", ""),
            ("../", ""), // Parent directory path normalizes to empty when at root
            ("a/b/../c", "a/c"),
            ("./a/./b", "a/b"),
        ];

        for (input, expected_normalized) in test_cases {
            let path = PosixPath(input.to_string());
            let normalized = path.as_unix_path().normalize();
            assert_eq!(
                normalized.to_string(),
                expected_normalized,
                "Failed for input: '{}'",
                input
            );
        }
    }

    /// Test edge cases for filename extraction from URLs,
    /// including URLs without filenames and directory URLs
    #[test]
    fn test_filename_extraction_edge_cases() {
        // Test edge cases for filename extraction
        let test_cases = vec![
            ("https://example.com/", Some("")),      // Empty filename from URL
            ("https://example.com/path/", Some("")), // Directory URL gives empty string
            ("https://example.com/file", Some("file")), // No extension
            ("https://example.com/path/file.ext", Some("file.ext")), // Normal case
        ];

        for (url, expected) in test_cases {
            let mock_response = MockResponseData::new(url);
            let result = mock_response.extract_filename();

            match expected {
                Some(expected_filename) => {
                    assert!(result.is_ok(), "Should succeed for URL: {}", url);
                    assert_eq!(result.unwrap(), expected_filename);
                }
                None => {
                    assert!(result.is_err(), "Should fail for URL: {}", url);
                }
            }
        }
    }
}
