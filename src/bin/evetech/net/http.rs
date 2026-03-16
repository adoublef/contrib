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
use futures_util::{Stream, StreamExt, TryStreamExt, stream::iter};
use reqwest::{Client, StatusCode, header};
use serde::{Deserialize, Serialize};
use std::io;
use tokio::{io::duplex, spawn, sync::mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::ReaderStream;
use url::Url;

pub fn app(client: Client) -> Router {
    Router::new().route("/", get(handle_csv)).with_state(client)
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Order {
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
    let fut = spawn(async move {
        // create csv writer
        let mut wri = AsyncSerializer::from_writer(tx);
        // TODO - stream json body
        let regions = iter(
            client
                .get(base_url.join("/v1/universe/regions")?)
                .send()
                .await?
                .error_for_status()?
                .json::<Vec<u32>>()
                .await?,
        );

        let (tx, rx) = mpsc::channel(1);
        let fut_regions = spawn({
            let client = client.clone();
            let base_url = base_url.clone();
            async move {
                regions
                    .map(Ok::<_, anyhow::Error>)
                    .try_for_each_concurrent(1, |region| {
                        let tx = tx.clone();
                        let client = client.clone();
                        let base_url = base_url.clone();
                        async move {
                            let last = client
                                .head(base_url.join(&format!("/v1/markets/{region}/orders"))?)
                                .send()
                                .await?
                                .error_for_status()?
                                .headers()
                                .get("x-pages")
                                .context("Missing x-pages header")?
                                .to_str()?
                                .parse::<u32>()?;

                            for page in 1..=last {
                                tx.send((region, page)).await?;
                            }
                            Ok::<_, anyhow::Error>(())
                        }
                    })
                    .await?;
                Ok::<_, anyhow::Error>(())
            }
        });

        let pages = ReceiverStream::new(rx);
        let (tx, mut rx) = mpsc::channel(1);
        let fut_pages = spawn({
            let client = client.clone();
            let base_url = base_url.clone();
            async move {
                pages
                    .map(Ok::<_, anyhow::Error>)
                    .try_for_each_concurrent(1, |(region, page)| {
                        let tx = tx.clone();
                        let client = client.clone();
                        let base_url = base_url.clone();
                        async move {
                            let orders =
                                client
                                    .get(base_url.join(&format!(
                                        "/v1/markets/{region}/orders?page={page}"
                                    ))?)
                                    .send()
                                    .await?
                                    .error_for_status()?
                                    .json::<Vec<Order>>()
                                    .await?;

                            // while let broke this
                            for order in orders {
                                tx.send(order).await?;
                            }
                            Ok::<_, anyhow::Error>(())
                        }
                    })
                    .await?;
                Ok::<_, anyhow::Error>(())
            }
        });

        while let Some(order) = rx.recv().await {
            wri.serialize(&order).await?;
        }
        fut_regions.await??;
        fut_pages.await??;
        wri.flush().await?;

        Ok::<_, anyhow::Error>(())
    });
    let mut stream = ReaderStream::new(rx);
    try_stream! {
        while let Some(msg) = stream.next().await {
            let msg = msg?; // Result<Bytes, Error>
            yield msg
        }
        fut.await?.map_err(io::Error::other)?
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
        .header(header::CONTENT_TYPE, mime::TEXT_CSV.essence_str())
        .header(
            header::CONTENT_DISPOSITION,
            "attachment; filename=\"evetech.csv\"",
        )
        .status(StatusCode::OK)
        .body(Body::from_stream(stream))
        .unwrap()
}

#[cfg(test)]
mod test {
    use super::*;
    use anyhow::Context;
    use axum::{
        Json, Router,
        extract::Path,
        http::Response,
        routing::{get, head},
    };
    use csv_async::AsyncReader;
    use futures_util::TryStreamExt;
    use reqwest::{Client, StatusCode, header};
    use std::io;
    use tokio::{net::TcpListener, spawn, task::JoinHandle};
    use tokio_stream::StreamExt;
    use tokio_util::io::StreamReader;
    use url::Url;

    #[tokio::test]
    async fn test_csv_ok() -> anyhow::Result<()> {
        let num_regions = 1 << 2;
        let num_pages = 1 << 1;
        let num_orders = 1 << 2;

        let (client, api_url) = api_serve(num_regions, num_pages, num_orders).await?;
        let (client, mut url) = serve(client).await?;

        url.query_pairs_mut()
            .append_pair("base_url", api_url.as_str());

        let response = client.get(url).send().await?;
        assert_eq!(response.status(), StatusCode::OK);
        let headers = response.headers();
        let content_type = headers
            .get(header::CONTENT_TYPE)
            .context("Missing content-type header")?;
        assert_eq!(content_type, mime::TEXT_CSV.as_ref());

        let mut rdr = AsyncReader::from_reader(StreamReader::new(
            response.bytes_stream().map_err(io::Error::other),
        ));
        let mut records = rdr.records();
        let mut num_records = 0;
        while let Some(record) = records.next().await {
            let record = record?;
            assert_eq!(record.len(), 12);
            num_records += 1;
        }
        assert_eq!(num_records, num_regions * num_pages * num_orders);

        Ok(())
    }

    async fn api_serve(
        num_regions: usize,
        num_pages: usize,
        num_orders: usize,
    ) -> anyhow::Result<(Client, Url)> {
        let listener = TcpListener::bind("0.0.0.0:0").await?;
        let addr = listener.local_addr()?;

        let client = Client::new();
        let url = Url::parse(&format!("http://{addr}"))?;

        // a vector of ids (stargeting at 10000)
        // a vector of orders

        let app = Router::new()
            // GET "/v1/universe/regions"
            .route(
                "/v1/universe/regions",
                get(async move |()| Json((1..=num_regions).map(|n| 10000 + n).collect::<Vec<_>>())),
            )
            // HEAD "/v1/markets/{region}/orders"
            .route(
                "/v1/markets/{region}/orders",
                head(async move |Path(_): Path<usize>| {
                    Response::builder()
                        .header("x-pages", num_pages)
                        .body(Body::empty())
                        .unwrap()
                }),
            )
            // GET "/v1/markets/{region}/orders?page={page}"
            .route(
                "/v1/markets/{region}/orders",
                get(async move |Path(_): Path<usize>| {
                    Json(
                        (1..=num_orders)
                            .map(|_| Order::default())
                            .collect::<Vec<_>>(),
                    )
                }),
            );

        spawn(async move {
            if let Err(err) = axum::serve(listener, app).await {
                eprintln!("server error: {err}");
            }
        });

        Ok((client, url))
    }

    async fn serve(api_client: Client) -> anyhow::Result<(Client, Url)> {
        let listener = TcpListener::bind("0.0.0.0:0").await?;
        let addr = listener.local_addr()?;

        let client = Client::new();
        let url = Url::parse(&format!("http://{addr}"))?;

        // spawn new process, how would i close it?
        spawn(async move {
            if let Err(err) = axum::serve(listener, app(api_client)).await {
                eprintln!("server error: {err}");
            }
        });

        Ok((client, url))
    }
}
