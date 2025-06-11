//
// Copyright (c) 2020 Nathan Fiedler
//
#![recursion_limit = "256"] // helps leptos watch?

//! The main application binary that starts the web server and spawns the
//! supervisor threads to manage the backups.

use actix_cors::Cors;
#[cfg(feature = "ssr")]
use actix_files::Files;
#[cfg(feature = "ssr")]
use actix_web::{
    error::InternalError, http, middleware, web, App, HttpResponse, HttpServer, Result,
};
use juniper::http::graphiql::graphiql_source;
use juniper::http::GraphQLRequest;
use log::{error, info};
use server::data::repositories::RecordRepositoryImpl;
use server::data::sources::EntityDataSourceImpl;
use server::domain::managers::backup::{Performer, PerformerImpl, Scheduler, SchedulerImpl};
use server::domain::managers::restore::{FileRestorer, FileRestorerImpl, Restorer, RestorerImpl};
use server::domain::managers::state::{self, StateStore, StateStoreImpl};
use server::domain::repositories::RecordRepository;
use server::domain::sources::EntityDataSource;
use server::preso::graphql;
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

fn file_restorer_factory(dbase: Arc<dyn RecordRepository>) -> Box<dyn FileRestorer> {
    Box::new(FileRestorerImpl::new(dbase))
}

// Application state store.
static STATE_STORE: LazyLock<Arc<dyn StateStore>> =
    LazyLock::new(|| Arc::new(StateStoreImpl::new()));
// File restore implementation.
static FILE_RESTORER: LazyLock<Arc<dyn Restorer>> = LazyLock::new(|| {
    Arc::new(RestorerImpl::new(
        STATE_STORE.clone(),
        file_restorer_factory,
    ))
});
// Actual performer of the backups.
static BACKUP_PERFORMER: LazyLock<Arc<dyn Performer>> =
    LazyLock::new(|| Arc::new(PerformerImpl::default()));
// Supervisor for managing the running of backups.
static SCHEDULER: LazyLock<Arc<dyn Scheduler>> = LazyLock::new(|| {
    Arc::new(SchedulerImpl::new(
        STATE_STORE.clone(),
        BACKUP_PERFORMER.clone(),
    ))
});
// Path to the database files.
static DB_PATH: LazyLock<PathBuf> = LazyLock::new(|| {
    let path = env::var("DB_PATH").unwrap_or_else(|_| DEFAULT_DB_PATH.to_owned());
    PathBuf::from(path)
});

#[cfg(feature = "ssr")]
async fn graphiql() -> Result<HttpResponse> {
    let html = graphiql_source("/graphql", None);
    Ok(HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(html))
}

#[cfg(feature = "ssr")]
async fn graphql(
    st: web::Data<(Arc<graphql::Schema>, leptos::config::LeptosOptions)>,
    data: web::Json<GraphQLRequest>,
) -> Result<HttpResponse> {
    let source = EntityDataSourceImpl::new(DB_PATH.as_path())
        .map_err(|e| InternalError::new(e, http::StatusCode::INTERNAL_SERVER_ERROR))?;
    let datasource: Arc<dyn EntityDataSource> = Arc::new(source);
    let state = STATE_STORE.clone();
    let processor = SCHEDULER.clone();
    let restorer = FILE_RESTORER.clone();
    let ctx = Arc::new(graphql::GraphContext::new(
        datasource, state, processor, restorer,
    ));
    let res = data.execute(&st.0, &ctx).await;
    let body = serde_json::to_string(&res)?;
    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(body))
}

// Start and stop the supervisor(s) based on application state changes.
fn manage_supervisors(state: &state::State, _previous: Option<&state::State>) {
    if state.supervisor == state::SupervisorState::Stopping {
        if let Err(err) = SCHEDULER.stop() {
            error!("error stopping supervisor: {}", err);
        }
    } else if state.supervisor == state::SupervisorState::Starting {
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
    if state.restorer == state::RestorerState::Stopping {
        if let Err(err) = FILE_RESTORER.stop() {
            error!("error stopping restorer: {}", err);
        }
    } else if state.restorer == state::RestorerState::Starting {
        match EntityDataSourceImpl::new(DB_PATH.as_path()) {
            Ok(datasource) => {
                let repo = RecordRepositoryImpl::new(Arc::new(datasource));
                let dbase: Arc<dyn RecordRepository> = Arc::new(repo);
                if let Err(err) = FILE_RESTORER.start(dbase) {
                    error!("error starting file restorer: {}", err);
                }
            }
            Err(err) => error!("error opening database: {}", err),
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

#[cfg(feature = "ssr")]
#[actix_rt::main]
async fn main() -> io::Result<()> {
    use leptos::config::get_configuration;
    use leptos_actix::{generate_route_list, LeptosRoutes};
    use server::preso::leptos::{shell, App};

    let conf = get_configuration(None).unwrap();
    let addr = conf.leptos_options.site_addr;

    dotenvy::dotenv().ok();
    env_logger::init();
    STATE_STORE.subscribe("super-manager", manage_supervisors);
    STATE_STORE.subscribe("backup-logger", log_state_changes);
    STATE_STORE.supervisor_event(state::SupervisorAction::Start);
    STATE_STORE.restorer_event(state::RestorerAction::Start);
    HttpServer::new(move || {
        let routes = generate_route_list(App);
        let leptos_options = &conf.leptos_options;
        let site_root = leptos_options.site_root.to_string();
        let schema = std::sync::Arc::new(graphql::create_schema());
        App::new()
            .app_data(web::Data::new((schema, leptos_options.to_owned())))
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
            // serve up the compiled static assets
            .service(
                Files::new("/pkg", format!("{site_root}/pkg"))
                    .use_etag(true)
                    .use_last_modified(true),
            )
            // serve up the raw static assets
            .service(
                Files::new("/assets", site_root)
                    .use_etag(true)
                    .use_last_modified(true),
            )
            .service(web::resource("/graphql").route(web::post().to(graphql)))
            .service(web::resource("/graphiql").route(web::get().to(graphiql)))
            .service(favicon)
            .service(
                web::resource("/liveness")
                    .route(web::get().to(|| HttpResponse::Ok()))
                    .route(web::head().to(|| HttpResponse::Ok())),
            )
            .leptos_routes(routes, {
                let leptos_options = leptos_options.clone();
                move || shell(leptos_options.clone())
            })
    })
    .bind(addr)?
    .run()
    .await
}

#[cfg(not(any(feature = "ssr", feature = "csr")))]
pub fn main() {
    // no client-side main function
    // unless we want this to work with e.g., Trunk for pure client-side testing
    // see lib.rs for hydration function instead
    // see optional feature `csr` instead
}

#[cfg(all(not(feature = "ssr"), feature = "csr"))]
pub fn main() {
    // a client-side main function is required for using `trunk serve`
    // prefer using `cargo leptos serve` instead
    // to run: `trunk serve --open --features csr`
    use crate::preso::leptos::*;
    console_error_panic_hook::set_once();
    leptos::mount_to_body(App);
}

#[cfg(feature = "ssr")]
#[actix_web::get("favicon.ico")]
async fn favicon(
    st: web::Data<(Arc<graphql::Schema>, leptos::config::LeptosOptions)>,
) -> actix_web::Result<actix_files::NamedFile> {
    let site_root = &st.1.site_root;
    Ok(actix_files::NamedFile::open(format!(
        "{site_root}/favicon.ico"
    ))?)
}
