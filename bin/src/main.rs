mod config;

use figment::{Figment, providers::Env};
use opendal::Operator;
use opendal::layers::LoggingLayer;
use restate_downloader::with_store::Downloader as DownloaderWithStore;
use restate_downloader::with_store::DownloaderImpl as DownloaderWithStoreImpl;
use restate_downloader::without_store::Downloader as DownloaderWithoutStore;
use restate_downloader::without_store::DownloaderImpl as DownloaderWithoutStoreImpl;
use restate_sdk::{endpoint::Endpoint, http_server::HttpServer};

use crate::config::Settings;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let settings: Settings = Figment::new()
        .merge(Env::raw().split("__"))
        .extract()
        .unwrap();

    print!("Settings: {:?}", settings);

    // Get port from environment variable or use default
    let port = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(9080);

    let bind_addr = format!("0.0.0.0:{}", port);
    tracing::info!("Starting file transfer service on {}", bind_addr);

    let client = reqwest::Client::builder()
        // .user_agent(&config.user_agent)
        // .redirect(reqwest::redirect::Policy::limited(config.max_redirects))
        // .timeout(Duration::from_secs(if timeout > 0 {
        //     timeout
        // } else {
        //     config.default_timeout_secs
        // }))
        .build()
        .unwrap();

    let mut endpoint = Endpoint::builder();

    if let Some(store_url) = settings.store.url {
        let operator = Operator::from_uri(store_url.to_string())
            .unwrap()
            .layer(LoggingLayer::default());
        let service = DownloaderWithStoreImpl::new(client, operator);

        endpoint = endpoint.bind_with_options(service.serve(), settings.restate.service.into())
    } else {
        let service = DownloaderWithoutStoreImpl::new(client);

        endpoint = endpoint.bind_with_options(service.serve(), settings.restate.service.into())
    }

    // Create and start the HTTP server
    HttpServer::new(endpoint.build())
        .listen_and_serve(bind_addr.parse().unwrap())
        .await;
}
