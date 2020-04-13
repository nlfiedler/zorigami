//
// Copyright (c) 2020 Nathan Fiedler
//

//! The main application binary that starts the web server and spawns the
//! supervisor threads to manage the backups.

use actix_cors::Cors;
use actix_files::{Files, NamedFile};
use actix_web::{http, middleware, web, App, HttpRequest, HttpResponse, HttpServer, Result};
use env_logger;
use failure::err_msg;
use juniper::http::graphiql::graphiql_source;
use juniper::http::GraphQLRequest;
use lazy_static::lazy_static;
use log::info;
use std::env;
use std::io;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use zorigami::core;
use zorigami::database::Database;
use zorigami::engine;
use zorigami::schema;
use zorigami::state;
use zorigami::supervisor;

lazy_static! {
    // Path to the database files.
    static ref DB_PATH: PathBuf = {
        dotenv::dotenv().ok();
        let path = env::var("DB_PATH").unwrap_or_else(|_| "tmp/database".to_owned());
        PathBuf::from(path)
    };
    // Path to the static web files.
    static ref STATIC_PATH: PathBuf = {
        let path = env::var("STATIC_FILES").unwrap_or_else(|_| "./web/".to_owned());
        PathBuf::from(path)
    };
    // Path of the fallback page for web requests.
    static ref DEFAULT_INDEX: PathBuf = {
        let mut path = STATIC_PATH.clone();
        path.push("index.html");
        path
    };
}

fn graphiql() -> HttpResponse {
    let html = graphiql_source("/graphql");
    HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(html)
}

async fn graphql(
    st: web::Data<Arc<schema::Schema>>,
    data: web::Json<GraphQLRequest>,
) -> Result<HttpResponse> {
    let ctx = Database::new(DB_PATH.as_path()).unwrap();
    let res = data.execute(&st, &ctx);
    let body = serde_json::to_string(&res)?;
    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(body))
}

async fn restore(info: web::Path<(String, String, String)>) -> Result<NamedFile> {
    let dbase = Database::new(DB_PATH.as_path())?;
    let dataset = dbase
        .get_dataset(info.0.as_ref())?
        .ok_or_else(|| err_msg(format!("missing dataset: {:?}", info.0)))?;
    let passphrase = core::get_passphrase();
    let checksum = core::Checksum::from_str(&info.1)?;
    let mut outfile = PathBuf::from(&dataset.workspace);
    outfile.push(info.2.clone());
    engine::restore_file(&dbase, &dataset, &passphrase, checksum, &outfile)?;
    // NamedFile does everything we need from here.
    let file = NamedFile::open(&outfile)?;
    Ok(file)
}

fn log_state_changes(state: &state::State) {
    for (key, backup) in state.active_datasets() {
        if let Some(end_time) = backup.end_time() {
            // the backup finished recently, log one last entry
            let sys_time = chrono::Utc::now();
            let interval = sys_time - end_time;
            if interval.num_seconds() < 60 {
                info!(
                    "complete for {}: packs: {}, files: {}",
                    key,
                    backup.packs_uploaded(),
                    backup.files_uploaded()
                );
            }
        } else {
            // this backup is not yet finished
            info!(
                "progress for {}: packs: {}, files: {}",
                key,
                backup.packs_uploaded(),
                backup.files_uploaded()
            );
        }
    }
}

// All requests that fail to match anything else will be directed to the index
// page, where the client-side code will handle the routing and "page not found"
// error condition.
async fn default_index(_req: HttpRequest) -> Result<NamedFile> {
    let file = NamedFile::open(DEFAULT_INDEX.as_path())?;
    Ok(file.use_last_modified(true))
}

#[actix_rt::main]
async fn main() -> io::Result<()> {
    env_logger::init();
    state::subscribe("main-logger", log_state_changes);
    supervisor::start(DB_PATH.clone()).unwrap();
    let host = env::var("HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
    let port = env::var("PORT").unwrap_or_else(|_| "8080".to_owned());
    let addr = format!("{}:{}", host, port);
    let schema = std::sync::Arc::new(schema::create_schema());
    info!("listening on http://{}/...", addr);
    HttpServer::new(move || {
        App::new()
            .data(schema.clone())
            .wrap(middleware::Logger::default())
            .wrap(
                // Respond to OPTIONS requests for CORS support, which is common
                // with some GraphQL clients, including the Dart package.
                Cors::new()
                    .allowed_methods(vec!["GET", "POST"])
                    .allowed_headers(vec![http::header::AUTHORIZATION, http::header::ACCEPT])
                    .allowed_header(http::header::CONTENT_TYPE)
                    .max_age(3600)
                    .finish(),
            )
            .service(web::resource("/graphql").route(web::post().to(graphql)))
            .service(web::resource("/graphiql").route(web::get().to(graphiql)))
            .service(
                web::resource("/restore/{dataset}/{checksum}/{filename}")
                    .route(web::get().to(restore)),
            )
            .service(Files::new("/", STATIC_PATH.clone()).index_file("index.html"))
            .default_service(web::get().to(default_index))
    })
    .bind(addr)?
    .run()
    .await
}
