use crate::encoding::html::{anchors, images};
use anyhow::format_err;
use async_stream::try_stream;
use async_zip::{Compression, ZipEntryBuilder, tokio::write::ZipFileWriter};
use axum::{
    Router,
    body::{Body, Bytes},
    extract::{Query, State},
    response::Response,
    routing::get,
};
use chrono::Utc;
use futures_util::StreamExt;
use futures_util::{Stream, TryStreamExt};
use reqwest::{Client, StatusCode, header};
use serde::Deserialize;
use std::{io, pin::Pin};
use tokio::{
    io::{AsyncWriteExt, copy, duplex},
    spawn,
    sync::mpsc,
    task::spawn_blocking,
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::{ReaderStream, SyncIoBridge};
use tokio_util::{compat::FuturesAsyncWriteCompatExt, io::StreamReader};
use url::Url;

pub fn app(client: Client) -> Router {
    Router::new()
        .route("/", get(handle_zip_series))
        .with_state(client)
}

async fn zip_chapter(
    client: Client,
    mut chapter_url: Url,
    concurrency: usize,
) -> Pin<Box<impl Stream<Item = Result<Bytes, io::Error>>>> {
    let (rx, tx) = duplex(4 * 1 << 10);
    let fut = spawn(async move {
        let mut series_zip = ZipFileWriter::with_tokio(tx).force_zip64();

        chapter_url
            .path_segments_mut()
            .map_err(|_| format_err!("Invalid path segments"))?
            .push("images");
        let response = client
            .get(chapter_url)
            .send()
            .await?
            .error_for_status()?
            .bytes_stream();

        let (tx, rx) = mpsc::channel(1);
        let worker = spawn_blocking(move || {
            let stream = SyncIoBridge::new(StreamReader::new(response.map_err(io::Error::other)));
            for res in images(stream) {
                let res = res?;
                tx.blocking_send(res).unwrap(); // we dont like unwrap here
            }
            Ok::<_, anyhow::Error>(())
        });
        // use rx
        let stream = ReceiverStream::new(rx);
        let mut stream = stream
            .map(move |image_url| {
                let client = client.clone();
                async move {
                    let response = client
                        .get(image_url)
                        .send()
                        .await?
                        .error_for_status()?
                        .bytes()
                        .await?;
                    // let content_length = response.content_length().unwrap_or(0);
                    // let response_stream =
                    //     &mut Limited::new(response.bytes_stream(), content_length as usize);

                    Ok::<_, anyhow::Error>(response)
                }
            })
            .buffer_unordered(concurrency) // how do i know this is working?
            .enumerate();

        while let Some((ix, image_body)) = stream.next().await {
            let mut image_body = image_body?;
            let mut chapter_entry = series_zip
                .write_entry_stream(
                    ZipEntryBuilder::new(format!("image-{ix}.png").into(), Compression::Stored)
                        .last_modification_date(Utc::now().into()),
                )
                .await?
                .compat_write();

            chapter_entry.write_all(&mut image_body).await?;
            chapter_entry.into_inner().close().await?;
        }
        worker.await??;
        series_zip.close().await?;

        Ok::<_, anyhow::Error>(())
    });

    let mut stream = ReaderStream::new(rx);
    // https://without.boats/blog/pin/
    Box::pin(try_stream! {
        while let Some(msg) = stream.next().await {
            let msg = msg?;
            yield msg
        }
        fut.await?.map_err(io::Error::other)?
    })
}

async fn zip_series(
    client: Client,
    mut series_url: Url,
) -> impl Stream<Item = Result<Bytes, io::Error>> {
    let (rx, tx) = duplex(4 * 1 << 10);
    let fut = spawn(async move {
        let mut series_zip = ZipFileWriter::with_tokio(tx).force_zip64();

        series_url
            .path_segments_mut()
            .map_err(|_| format_err!("Invalid path segments"))?
            .push("full-chapter-list");
        // make net query
        let response = client
            .get(series_url)
            .send()
            .await?
            .error_for_status()?
            .bytes_stream();

        let (tx, rx) = mpsc::channel(1);
        let worker = spawn_blocking(move || {
            let stream = SyncIoBridge::new(StreamReader::new(response.map_err(io::Error::other)));
            for res in anchors(stream) {
                let res = res?;
                tx.blocking_send(res).unwrap(); // we dont like unwrap here
            }
            Ok::<_, anyhow::Error>(())
        });

        let mut stream = ReceiverStream::new(rx).enumerate();
        while let Some((ix, chapter_url)) = stream.next().await {
            let mut chapter_stream =
                StreamReader::new(zip_chapter(client.clone(), chapter_url.clone(), 1).await);

            let mut chapter_entry = series_zip
                .write_entry_stream(
                    ZipEntryBuilder::new(format!("chapter-{ix}.zip").into(), Compression::Stored)
                        .last_modification_date(Utc::now().into()),
                )
                .await?
                .compat_write();

            copy(&mut chapter_stream, &mut chapter_entry).await?;
            chapter_entry.into_inner().close().await?;
        }
        worker.await??;
        series_zip.close().await?;

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
struct ZipSeriesParams {
    series_url: Url, // Url does not satisfy Deserialize?
}

async fn handle_zip_series(
    State(client): State<Client>,
    Query(ZipSeriesParams { series_url }): Query<ZipSeriesParams>,
) -> Response<Body> {
    let stream = zip_series(client, series_url).await;
    // where do i handle the error
    Response::builder()
        .header(
            header::CONTENT_TYPE,
            mime::APPLICATION_OCTET_STREAM.essence_str(),
        )
        .header(
            header::CONTENT_DISPOSITION,
            "attachment; filename=\"series.zip\"",
        )
        .status(StatusCode::OK)
        .body(Body::from_stream(stream))
        .unwrap()
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::html::template;
    use anyhow::Context;
    use async_zip::tokio::read::seek::ZipFileReader;
    use axum::{extract::Path, response::Html, routing::get_service};
    use futures_util::TryStreamExt;
    use reqwest::{Client, StatusCode, Url};
    use tokio::{fs::File, io::BufReader, net::TcpListener};
    use tokio_util::{compat::TokioAsyncReadCompatExt, io::StreamReader};
    use tower_http::services::ServeFile;

    #[tokio::test]
    /// [See more](https://github.com/tokio-rs/axum/blob/main/examples/testing/src/main.rs)
    async fn test_zip_series_ok() -> anyhow::Result<()> {
        let num_chapters = 1 << 4;
        let num_images = 1 << 0;

        let (client, api_url) = api_serve(num_chapters, num_images).await?;
        let (client, mut url) = serve(client).await?;

        let series_url = api_url.join("series/1")?;
        url.query_pairs_mut()
            .append_pair("series_url", series_url.as_str());

        let response = client.get(url).send().await?;
        assert_eq!(response.status(), StatusCode::OK);
        let headers = response.headers();
        let content_type = headers
            .get(header::CONTENT_TYPE)
            .context("Missing content-type header")?;
        assert_eq!(content_type, mime::APPLICATION_OCTET_STREAM.as_ref());

        // write to a temp file and then parse it
        {
            let mut file = File::create("test.zip").await?;
            let mut stream = StreamReader::new(response.bytes_stream().map_err(io::Error::other));

            assert!(copy(&mut stream, &mut file).await? > 0);
        };

        let mut num_files = 0;
        let archive = File::open("test.zip").await?;
        let archive = BufReader::new(archive).compat();
        let reader = ZipFileReader::new(archive).await?;
        for index in 0..reader.file().entries().len() {
            let entry = reader
                .file()
                .entries()
                .get(index)
                .ok_or_else(|| anyhow::format_err!("No entry found"))?;
            assert_eq!(entry.dir()?, false);
            // open the zip as file
            num_files += 1;
            // Read the content of the zip file
        }
        assert_eq!(num_files, num_chapters);

        Ok(())
    }

    async fn api_serve(num_chapters: usize, num_images: usize) -> anyhow::Result<(Client, Url)> {
        let listener = TcpListener::bind("0.0.0.0:0").await?;
        let addr = listener.local_addr()?;

        let client = Client::new();
        let url = Url::parse(&format!("http://{addr}"))?;

        let app = Router::new()
            .route(
                "/series/{series}/full-chapter-list",
                get(async move |State(url): State<_>| Html(template::series(num_chapters, url))),
            )
            .route(
                "/chapters/{chapters}/images",
                get(
                    async move |State(url): State<_>, Path(chapter_id): Path<_>| {
                        Html(template::chapter(num_images, url, chapter_id))
                    },
                ),
            )
            .route(
                "/images/{image}",
                get_service(ServeFile::new("assets/image.jpg")),
            )
            .with_state(url.clone());

        // spawn new process, how would i close it?
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
