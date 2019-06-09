//
// Copyright (c) 2019 Nathan Fiedler
//

//! The main application binary that starts the web server and spawns the
//! supervisor threads to manage the backups.

use actix_files as afs;
use actix_web::{middleware, web, App, Error, HttpResponse, HttpServer};
use env_logger;
use futures::future::Future;
use juniper::http::graphiql::graphiql_source;
use juniper::http::GraphQLRequest;
use lazy_static::lazy_static;
use log::info;
use std::env;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use zorigami::database::Database;
use zorigami::state;

mod schema;

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
        let ctx = Database::new(&DB_PATH).unwrap();
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

fn log_state_changes(state: &state::State) {
    let keys = state.active_datasets();
    for key in keys {
        let backup = state.backups(&key).unwrap();
        if let Some(end_time) = backup.end_time() {
            // the backup finished recently, log one last entry
            let sys_time = std::time::SystemTime::now();
            if let Ok(interval) = sys_time.duration_since(end_time) {
                if interval.as_secs() < 60 {
                    info!(
                        "complete for {}: packs: {}, files: {}",
                        &key,
                        backup.packs_uploaded(),
                        backup.files_uploaded()
                    );
                }
            }
        } else {
            // this backup is not yet finished
            info!(
                "progress for {}: packs: {}, files: {}",
                &key,
                backup.packs_uploaded(),
                backup.files_uploaded()
            );
        }
    }
}

pub fn main() -> io::Result<()> {
    env_logger::init();
    state::subscribe("main-logger", log_state_changes);
    let port = env::var("PORT").unwrap_or_else(|_| "8080".to_owned());
    let addr = format!("127.0.0.1:{}", port);
    let schema = std::sync::Arc::new(schema::create_schema());
    info!("listening on http://{}/...", addr);
    HttpServer::new(move || {
        App::new()
            .data(schema.clone())
            .wrap(middleware::Logger::default())
            .service(web::resource("/graphql").route(web::post().to_async(graphql)))
            .service(web::resource("/graphiql").route(web::get().to(graphiql)))
            .service(afs::Files::new("/", "./public/").index_file("index.html"))
    })
    .bind(addr)?
    .run()
}
