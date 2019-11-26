//
// Copyright (c) 2019 Nathan Fiedler
//

//! The main application binary that starts the web server and spawns the
//! supervisor threads to manage the backups.

use actix_files as afs;
use actix_web::{middleware, web, App, Error, HttpRequest, HttpResponse, HttpServer};
use env_logger;
use failure::err_msg;
use futures::future::Future;
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
}

fn graphiql() -> HttpResponse {
    let html = graphiql_source("/graphql");
    HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(html)
}

fn graphql(
    st: web::Data<Arc<schema::Schema>>,
    data: web::Json<GraphQLRequest>,
) -> impl Future<Item = HttpResponse, Error = Error> {
    web::block(move || {
        let ctx = Database::new(DB_PATH.as_path()).unwrap();
        let res = data.execute(&st, &ctx);
        Ok::<_, serde_json::error::Error>(serde_json::to_string(&res)?)
    })
    .map_err(Error::from)
    .and_then(|body| {
        Ok(HttpResponse::Ok()
            .content_type("application/json")
            .body(body))
    })
}

fn restore(
    info: web::Path<(String, String, String)>,
) -> impl Future<Item = afs::NamedFile, Error = Error> {
    web::block(move || {
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
        let file = afs::NamedFile::open(&outfile)?;
        Ok::<_, failure::Error>(file)
    })
    .map_err(Error::from)
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
fn default_index(_req: HttpRequest) -> Result<afs::NamedFile, Error> {
    let file = afs::NamedFile::open("./public/index.html")?;
    Ok(file.use_last_modified(true))
}

pub fn main() -> io::Result<()> {
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
            .service(web::resource("/graphql").route(web::post().to_async(graphql)))
            .service(web::resource("/graphiql").route(web::get().to(graphiql)))
            .service(
                web::resource("/restore/{dataset}/{checksum}/{filename}")
                    .route(web::get().to_async(restore)),
            )
            .service(afs::Files::new("/", "./public/").index_file("index.html"))
            .default_service(web::get().to(default_index))
    })
    .bind(addr)?
    .run()
}
