//
// Copyright (c) 2020 Nathan Fiedler
//

//! The main application binary that starts the web server and spawns the
//! supervised actors to manage the various background operations.

use actix_cors::Cors;
use actix_files::{Files, NamedFile};
use actix_web::{
    App, HttpResponse, HttpServer, Result, error::InternalError, http, middleware, web,
};
use juniper::http::GraphQLRequest;
use juniper::http::graphiql::graphiql_source;
use log::{error, info};
use server::data::repositories::RecordRepositoryImpl;
use server::data::sources::EntityDataSourceImpl;
use server::domain::repositories::RecordRepository;
use server::domain::sources::EntityDataSource;
use server::preso::graphql;
use server::shared::state::{self, StateStore, StateStoreImpl};
use server::tasks::leader::{RingLeader, RingLeaderImpl};
use server::tasks::schedule::{Scheduler, SchedulerImpl};
use std::env;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};

// When running in test mode, the cwd is the server directory.
#[cfg(test)]
static DEFAULT_DB_PATH: &str = "../tmp/test/database";

// Running in debug/release mode we assume cwd is root directory.
#[cfg(not(test))]
static DEFAULT_DB_PATH: &str = "./tmp/database";

// Path to the database files.
static DB_PATH: LazyLock<PathBuf> = LazyLock::new(|| {
    let path = std::env::var("DB_PATH").unwrap_or_else(|_| DEFAULT_DB_PATH.to_owned());
    PathBuf::from(path)
});

// Application state store.
static STATE_STORE: LazyLock<Arc<dyn StateStore>> =
    LazyLock::new(|| Arc::new(StateStoreImpl::new()));

// Ring leader implementation that manages the restore, backup, and prune requests.
static RING_LEADER: LazyLock<Arc<dyn RingLeader>> =
    LazyLock::new(|| Arc::new(RingLeaderImpl::new(STATE_STORE.clone())));

// Scheduler implementation that sends backup and prune requests to the ring
// leader according to the schedules defined for each of the datasets. Runs
// every 5 minutes to check if any datasets need backing up.
static SCHEDULER_INTERVAL: u64 = 300_000;
static SCHEDULER: LazyLock<Arc<dyn Scheduler>> = LazyLock::new(|| {
    Arc::new(SchedulerImpl::new(
        STATE_STORE.clone(),
        RING_LEADER.clone(),
        SCHEDULER_INTERVAL,
    ))
});

async fn graphiql() -> Result<HttpResponse> {
    let html = graphiql_source("/graphql", None);
    Ok(HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(html))
}

async fn graphql(
    st: web::Data<Arc<graphql::Schema>>,
    data: web::Json<GraphQLRequest>,
) -> Result<HttpResponse> {
    let source = EntityDataSourceImpl::new(DB_PATH.as_path())
        .map_err(|e| InternalError::new(e, http::StatusCode::INTERNAL_SERVER_ERROR))?;
    let datasource: Arc<dyn EntityDataSource> = Arc::new(source);
    let leader = RING_LEADER.clone();
    let ctx = Arc::new(graphql::GraphContext::new(datasource, leader));
    let res = data.execute(&st, &ctx).await;
    let body = serde_json::to_string(&res)?;
    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(body))
}

async fn index() -> actix_web::Result<NamedFile> {
    Ok(NamedFile::open("./dist/index.html")?)
}

// Start and stop the supervisor(s) based on application state changes.
fn manage_supervisors(state: &state::State, _previous: Option<&state::State>) {
    if state.scheduler == state::SchedulerState::Stopping {
        if let Err(err) = SCHEDULER.stop() {
            error!("error stopping supervisor: {}", err);
        }
    } else if state.scheduler == state::SchedulerState::Starting {
        match EntityDataSourceImpl::new(DB_PATH.as_path()) {
            Ok(datasource) => {
                let repo = RecordRepositoryImpl::new(Arc::new(datasource));
                let dbase: Arc<dyn RecordRepository> = Arc::new(repo);
                if let Err(err) = SCHEDULER.start(dbase) {
                    error!("error starting supervisor: {}", err);
                }
            }
            Err(err) => error!("error opening database: {}", err),
        }
    }
    if state.leader == state::LeaderState::Stopping {
        if let Err(err) = RING_LEADER.stop() {
            error!("error stopping restorer: {}", err);
        }
    } else if state.leader == state::LeaderState::Starting {
        match EntityDataSourceImpl::new(DB_PATH.as_path()) {
            Ok(datasource) => {
                let repo = RecordRepositoryImpl::new(Arc::new(datasource));
                let dbase: Arc<dyn RecordRepository> = Arc::new(repo);
                if let Err(err) = RING_LEADER.start(dbase) {
                    error!("error starting file restorer: {}", err);
                }
            }
            Err(err) => error!("error opening database: {}", err),
        }
    }
}

#[actix_rt::main]
async fn main() -> io::Result<()> {
    dotenvy::dotenv().ok();
    env_logger::init();

    if let Ok(path) = std::env::var("GENERATE_SDL") {
        // once the schema has been written, exit immediatly
        graphql::write_schema(&path)?;
        println!("GraphQL schema written to {path}");
        return Ok(());
    }

    STATE_STORE.subscribe("super-manager", manage_supervisors);
    STATE_STORE.scheduler_event(state::SchedulerAction::Start);
    STATE_STORE.leader_event(state::LeaderAction::Start);

    let host = env::var("HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
    let port = env::var("PORT").unwrap_or_else(|_| "3000".to_owned());
    let addr = format!("{}:{}", host, port);
    info!("listening on {}", addr);

    let schema = std::sync::Arc::new(graphql::create_schema());
    HttpServer::new(move || {
        // This block is called for every thread that is started, so anything
        // that should be run once is moved outside and cloned in.
        App::new()
            .app_data(web::Data::new(schema.clone()))
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
            .service(
                Files::new("/assets", "./dist/assets")
                    .use_etag(true)
                    .use_last_modified(true),
            )
            .service(
                Files::new("/fontawesome", "./dist/fontawesome")
                    .use_etag(true)
                    .use_last_modified(true),
            )
            .service(web::resource("/graphql").route(web::post().to(graphql)))
            .service(web::resource("/graphiql").route(web::get().to(graphiql)))
            .service(favicon)
            .service(
                web::resource("/liveness")
                    .route(web::get().to(HttpResponse::Ok))
                    .route(web::head().to(HttpResponse::Ok)),
            )
            .default_service(web::get().to(index))
    })
    .bind(addr)?
    .run()
    .await
}

#[actix_web::get("favicon.ico")]
async fn favicon() -> actix_web::Result<actix_files::NamedFile> {
    Ok(actix_files::NamedFile::open("./dist/favicon.ico")?)
}
