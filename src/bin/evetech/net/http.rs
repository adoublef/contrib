use std::io;

use anyhow::Context;
use async_stream::try_stream;
use axum::{
    Router,
    body::{Body, Bytes},
    extract::{Query, State},
    http::Response,
    routing::get,
};
use csv_async::AsyncSerializer;
use futures_util::{Stream, stream::iter};
use reqwest::{Client, StatusCode, header};
use serde::{Deserialize, Serialize};
use tokio::{io::duplex, spawn};
use tokio_stream::StreamExt;
use tokio_util::io::ReaderStream;
use url::Url;

pub fn app(client: Client) -> Router {
    Router::new().route("/", get(handle_csv)).with_state(client)
}

#[derive(Debug, Serialize, Deserialize)]
struct Order {
    duration: i64,
    is_buy_order: bool,
    issued: String,
    location_id: i64,
    min_volume: i64,
    order_id: i64,
    price: f64,
    range: String,
    system_id: i64,
    type_id: i64,
    volume_remain: i64,
    volume_total: i64,
}

async fn csv_reader(client: Client, base_url: Url) -> impl Stream<Item = Result<Bytes, io::Error>> {
    let (rx, tx) = duplex(4 * 1 << 10);
    let worker = spawn(async move {
        // create csv writer
        let mut wri = AsyncSerializer::from_writer(tx);
        // TODO - stream json body
        let mut regions = client
            .get(base_url.join("/v1/universe/regions")?)
            .send()
            .await?
            .error_for_status()?
            .json::<Vec<u32>>()
            .await?;

        while let Some(id) = iter(&mut regions).next().await {
            // ... get the max last page
            let last = client
                .head(base_url.join(&format!("/v1/markets/{id}/orders"))?)
                .send()
                .await?
                .error_for_status()?
                .headers()
                .get("x-pages")
                .context("Missing x-pages header")?
                .to_str()?
                .parse::<u32>()?;
            // ... for each page, we query the order body
            while let Some(page) = iter(1..=last).next().await {
                // Todo use a body stream?
                let orders = client
                    .get(
                        base_url
                            .clone()
                            .join(&format!("v1/markets/{id}/orders?page={page}"))?,
                    )
                    .send()
                    .await?
                    .error_for_status()?
                    .json::<Vec<Order>>()
                    .await?;
                // write to csv
                for order in orders {
                    wri.serialize(&order).await?;
                }
            }
        }
        // close csv writer
        wri.flush().await?;
        Ok::<_, anyhow::Error>(())
    });
    let mut stream = ReaderStream::new(rx);
    try_stream! {
        while let Some(msg) = stream.next().await {
            let msg = msg?; // Result<Bytes, Error>
            yield msg
        }
        worker.await?.map_err(io::Error::other)?
    }
}

#[derive(Deserialize)]
struct CsvParams {
    base_url: Url, // Url does not satisfy Deserialize?
}

async fn handle_csv(
    State(client): State<Client>,
    Query(CsvParams { base_url }): Query<CsvParams>,
) -> Response<Body> {
    let stream = csv_reader(client, base_url).await;
    Response::builder()
        .header(header::CONTENT_TYPE, mime::TEXT_CSV_UTF_8.essence_str())
        .header(
            header::CONTENT_DISPOSITION,
            "attachment; filename=\"evetech.csv\"",
        )
        .status(StatusCode::OK)
        .body(Body::from_stream(stream))
        .unwrap()
}
