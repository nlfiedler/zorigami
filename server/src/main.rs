//
// Copyright (c) 2020 Nathan Fiedler
//

//! The main application binary that starts the web server and spawns the
//! supervisor threads to manage the backups.

use actix_cors::Cors;
use actix_files::{Files, NamedFile};
use actix_web::{
    App, HttpResponse, HttpServer, Result, error::InternalError, http, middleware, web,
};
use juniper::http::GraphQLRequest;
use juniper::http::graphiql::graphiql_source;
use log::{error, info};
use std::env;
use std::io;
use std::sync::{Arc, LazyLock};
use zorigami::data::repositories::RecordRepositoryImpl;
use zorigami::data::sources::EntityDataSourceImpl;
use zorigami::domain::repositories::RecordRepository;
use zorigami::domain::sources::EntityDataSource;
use zorigami::preso::graphql;
use zorigami::tasks::backup::{Performer, PerformerImpl, Scheduler, SchedulerImpl};
use zorigami::tasks::restore::{FileRestorer, FileRestorerImpl, Restorer, RestorerImpl};
use zorigami::tasks::state;
use zorigami::{DB_PATH, STATE_STORE};

fn file_restorer_factory(dbase: Arc<dyn RecordRepository>) -> Box<dyn FileRestorer> {
    Box::new(FileRestorerImpl::new(dbase))
}

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
    let state = STATE_STORE.clone();
    let processor = SCHEDULER.clone();
    let restorer = FILE_RESTORER.clone();
    let ctx = Arc::new(graphql::GraphContext::new(
        datasource, state, processor, restorer,
    ));
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

#[actix_rt::main]
async fn main() -> io::Result<()> {
    dotenvy::dotenv().ok();
    env_logger::init();
    STATE_STORE.subscribe("super-manager", manage_supervisors);
    STATE_STORE.subscribe("backup-logger", log_state_changes);
    STATE_STORE.supervisor_event(state::SupervisorAction::Start);
    STATE_STORE.restorer_event(state::RestorerAction::Start);

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
