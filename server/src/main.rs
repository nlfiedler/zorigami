//
// Copyright (c) 2020 Nathan Fiedler
//

//! The main application binary that starts the web server and spawns the
//! supervisor threads to manage the backups.

use actix_cors::Cors;
use actix_files::{Files, NamedFile};
use actix_web::{http, middleware, web, App, HttpRequest, HttpResponse, HttpServer, Result};
use env_logger;
use juniper::http::graphiql::graphiql_source;
use juniper::http::GraphQLRequest;
use lazy_static::lazy_static;
use log::{error, info};
use server::data::repositories::RecordRepositoryImpl;
use server::data::sources::{EntityDataSource, EntityDataSourceImpl};
use server::domain::managers::process;
use server::domain::managers::state::{self, StateStore, StateStoreImpl};
use server::domain::repositories::RecordRepository;
use server::preso::graphql;
use std::env;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

// When running in test mode, the cwd is the server directory.
#[cfg(test)]
static DEFAULT_DB_PATH: &str = "../tmp/test/database";

// Running in debug/release mode we assume cwd is root directory.
#[cfg(not(test))]
static DEFAULT_DB_PATH: &str = "./tmp/database";

// When running in test mode, the cwd is the server directory.
#[cfg(test)]
static DEFAULT_WEB_PATH: &str = "../web/";

// Running in debug/release mode we assume cwd is root directory.
#[cfg(not(test))]
static DEFAULT_WEB_PATH: &str = "./web/";

lazy_static! {
    // Application state store.
    static ref STATE_STORE: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
    // Supervisor for managing the running of backups.
    static ref PROCESSOR: Box<dyn process::Processor> = {
        Box::new(process::ProcessorImpl::new(STATE_STORE.clone()))
    };
    // Path to the database files.
    static ref DB_PATH: PathBuf = {
        dotenv::dotenv().ok();
        let path = env::var("DB_PATH").unwrap_or_else(|_| DEFAULT_DB_PATH.to_owned());
        PathBuf::from(path)
    };
    // Path to the static web files.
    static ref STATIC_PATH: PathBuf = {
        let path = env::var("STATIC_FILES").unwrap_or_else(|_| DEFAULT_WEB_PATH.to_owned());
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
    st: web::Data<Arc<graphql::Schema>>,
    data: web::Json<GraphQLRequest>,
) -> Result<HttpResponse> {
    let source = EntityDataSourceImpl::new(DB_PATH.as_path()).unwrap();
    let datasource: Arc<dyn EntityDataSource> = Arc::new(source);
    let state = STATE_STORE.clone();
    let ctx = Arc::new(graphql::GraphContext::new(datasource, state));
    let res = data.execute(&st, &ctx);
    let body = serde_json::to_string(&res)?;
    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(body))
}

// Start and stop the supervisor based on application state changes.
fn manage_supervisor(state: &state::State, _previous: Option<&state::State>) {
    if state.supervisor == state::SupervisorState::Stopping {
        if let Err(err) = PROCESSOR.stop() {
            error!("error stopping supervisor: {}", err);
        }
    } else if state.supervisor == state::SupervisorState::Starting {
        let datasource = EntityDataSourceImpl::new(DB_PATH.as_path()).unwrap();
        let repo = RecordRepositoryImpl::new(Arc::new(datasource));
        let dbase: Arc<dyn RecordRepository> = Arc::new(repo);
        if let Err(err) = PROCESSOR.start(dbase) {
            error!("error starting supervisor: {}", err);
        }
    }
}

// Log interesting changes in the application state (i.e. backup status).
fn log_state_changes(state: &state::State, _previous: Option<&state::State>) {
    // Ideally would compare state with previous to know if there is really
    // anything worth reporting, but that's more trouble than it's worth.
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
    STATE_STORE.subscribe("super-manager", manage_supervisor);
    STATE_STORE.subscribe("backup-logger", log_state_changes);
    STATE_STORE.supervisor_event(state::SupervisorAction::Start);
    let host = env::var("HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
    let port = env::var("PORT").unwrap_or_else(|_| "8080".to_owned());
    let addr = format!("{}:{}", host, port);
    info!("listening on http://{}/...", addr);
    HttpServer::new(move || {
        let schema = std::sync::Arc::new(graphql::create_schema());
        App::new()
            .data(schema)
            .wrap(middleware::Logger::default())
            .wrap(
                // Respond to OPTIONS requests for CORS support, which is common
                // with some GraphQL clients, including the Dart package.
                Cors::default()
                    .allow_any_origin()
                    .allowed_methods(vec!["GET", "POST"])
                    .allowed_headers(vec![http::header::AUTHORIZATION, http::header::ACCEPT])
                    .allowed_header(http::header::CONTENT_TYPE)
                    .max_age(3600),
            )
            .service(web::resource("/graphql").route(web::post().to(graphql)))
            .service(web::resource("/graphiql").route(web::get().to(graphiql)))
            .service(Files::new("/", STATIC_PATH.clone()).index_file("index.html"))
            .default_service(web::get().to(default_index))
    })
    .bind(addr)?
    .run()
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{test, web, App};

    #[actix_rt::test]
    async fn test_index_get() {
        // arrange
        let mut app =
            test::init_service(App::new().default_service(web::get().to(default_index))).await;
        // act
        let req = test::TestRequest::default().to_request();
        let resp = test::call_service(&mut app, req).await;
        // assert
        assert!(resp.status().is_success());
    }
}
