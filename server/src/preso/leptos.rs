//
// Copyright (c) 2025 Nathan Fiedler
//
use crate::domain::entities::{Checksum, Dataset, Snapshot, SnapshotCount, Store};
use chrono::{DateTime, Local, NaiveDateTime, Utc};
use leptos::prelude::*;
use leptos_meta::*;
use leptos_router::components::*;
use leptos_router::path;

mod datasets;
mod home;
mod nav;
mod stores;

pub fn shell(options: LeptosOptions) -> impl IntoView {
    view! {
        <!DOCTYPE html>
        <html lang="en" data-theme="dark">
            <head>
                <meta charset="utf-8" />
                <meta name="viewport" content="width=device-width, initial-scale=1" />
                <AutoReload options=options.clone() />
                <HydrationScripts options />
                <MetaTags />
            </head>
            <body>
                <App />
            </body>
        </html>
    }
}

#[component]
pub fn App() -> impl IntoView {
    // Provides context that manages stylesheets, titles, meta tags, etc.
    provide_meta_context();

    view! {
        <Stylesheet id="leptos" href="/pkg/server.css" />
        <Stylesheet href="/assets/fontawesome/css/all.min.css" />
        <Title text="Zorigami" />
        <Router>
            <main>
                <Routes fallback=NotFound>
                    <Route path=path!("") view=home::HomePage />
                    <ParentRoute path=path!("/datasets") view=datasets::DatasetsPage>
                        <Route path=path!(":id") view=datasets::DatasetDetails />
                        <Route
                            path=path!("")
                            view=|| {
                                view! {
                                    <div class="m-4">
                                        <p class="subtitle is-5">
                                            Select a data set to view details.
                                        </p>
                                    </div>
                                }
                            }
                        />
                    </ParentRoute>
                    <ParentRoute path=path!("/stores") view=stores::StoresPage>
                        <Route path=path!(":id") view=stores::StoreDetails />
                        <Route
                            path=path!("")
                            view=|| {
                                view! {
                                    <div class="m-4">
                                        <p class="subtitle is-5">Select a store to view details.</p>
                                    </div>
                                }
                            }
                        />
                    </ParentRoute>
                </Routes>
            </main>
        </Router>
    }
}

/// 404 - Not Found
#[component]
fn NotFound() -> impl IntoView {
    // set an HTTP status code 404 this is feature gated because it can only be
    // done during initial server-side rendering if you navigate to the 404 page
    // subsequently, the status code will not be set because there is not a new
    // HTTP request to the server
    #[cfg(feature = "ssr")]
    {
        // this can be done inline because it's synchronous if it were async,
        // we'd use a server function
        let resp = expect_context::<leptos_actix::ResponseOptions>();
        resp.set_status(actix_web::http::StatusCode::NOT_FOUND);
    }

    view! {
        <nav::NavBar />
        <section class="section">
            <h1 class="title">Page not found</h1>
            <h2 class="subtitle">This is not the page you are looking for.</h2>
            <div class="content">
                <p>Try using the navigation options above.</p>
            </div>
        </section>
    }
}

#[cfg(feature = "ssr")]
pub mod ssr {
    use crate::data::repositories::RecordRepositoryImpl;
    use crate::data::sources::EntityDataSourceImpl;
    use crate::domain::managers::state::StateStore;
    use crate::domain::sources::EntityDataSource;
    use server_fn::error::ServerFnErrorErr;
    use server_fn::ServerFnError;
    use std::sync::Arc;

    /// Construct a repository implementation for the database.
    pub fn db() -> Result<RecordRepositoryImpl, ServerFnError> {
        let source = EntityDataSourceImpl::new(crate::DB_PATH.as_path())
            .map_err(|e| ServerFnErrorErr::ServerError(e.to_string()))?;
        let datasource: Arc<dyn EntityDataSource> = Arc::new(source);
        let repo = RecordRepositoryImpl::new(datasource);
        Ok(repo)
    }

    /// Retrieve a reference to the application state.
    pub fn app_state() -> Arc<dyn StateStore> {
        crate::STATE_STORE.clone()
    }
}

/// Retrieve all pack stores.
#[leptos::server]
pub async fn stores() -> Result<Vec<Store>, ServerFnError> {
    use crate::domain::usecases::get_stores::GetStores;
    use crate::domain::usecases::{NoParams, UseCase};
    use leptos::server_fn::error::ServerFnErrorErr;

    let repo = ssr::db()?;
    let usecase = GetStores::new(Box::new(repo));
    let params = NoParams {};
    let stores: Vec<Store> = usecase
        .call(params)
        .map_err(|e| ServerFnErrorErr::ServerError(e.to_string()))?;
    Ok(stores)
}

/// Retrieve all datasets.
#[leptos::server]
pub async fn datasets() -> Result<Vec<Dataset>, ServerFnError> {
    use crate::domain::usecases::get_datasets::GetDatasets;
    use crate::domain::usecases::{NoParams, UseCase};
    use leptos::server_fn::error::ServerFnErrorErr;

    let repo = ssr::db()?;
    let usecase = GetDatasets::new(Box::new(repo));
    let params = NoParams {};
    let datasets: Vec<Dataset> = usecase
        .call(params)
        .map_err(|e| ServerFnErrorErr::ServerError(e.to_string()))?;
    Ok(datasets)
}

// internal version of serverfn for server-side use
#[cfg(feature = "ssr")]
async fn get_dataset_0(id: String) -> Result<Option<Dataset>, ServerFnError> {
    use crate::domain::usecases::get_datasets::GetDatasets;
    use crate::domain::usecases::{NoParams, UseCase};
    use leptos::server_fn::error::ServerFnErrorErr;

    // calling get_datasets() to get one dataset is hardly efficient but for now
    // it is not bad enough to add another use case
    let repo = ssr::db()?;
    let usecase = GetDatasets::new(Box::new(repo));
    let params = NoParams {};
    let datasets: Vec<Dataset> = usecase
        .call(params)
        .map_err(|e| ServerFnErrorErr::ServerError(e.to_string()))?;
    for dataset in datasets.into_iter() {
        if dataset.id == id {
            return Ok(Some(dataset));
        }
    }
    Ok(None)
}

// internal version of serverfn for server-side use
#[cfg(feature = "ssr")]
async fn get_snapshot_0(digest: Checksum) -> Result<Option<Snapshot>, ServerFnError> {
    use crate::domain::usecases::get_snapshot::{GetSnapshot, Params};
    use crate::domain::usecases::UseCase;
    use leptos::server_fn::error::ServerFnErrorErr;

    let repo = ssr::db()?;
    let usecase = GetSnapshot::new(Box::new(repo));
    let params = Params::new(digest);
    let snapshot = usecase
        .call(params)
        .map_err(|e| ServerFnErrorErr::ServerError(e.to_string()))?;
    Ok(snapshot)
}

/// Convert a DateTime<Utc> to a NaiveDateTime for the local timezone.
pub fn convert_utc_to_local(datetime: DateTime<Utc>) -> NaiveDateTime {
    // this is quite complicated for some reason
    let local_now = Local::now();
    let naive_utc = datetime.naive_utc();
    let datetime_local =
        DateTime::<Local>::from_naive_utc_and_offset(naive_utc, *local_now.offset());
    datetime_local.naive_local()
}

/// Describe current status of the dataset, including backup progress.
#[leptos::server]
pub async fn dataset_status(id: String) -> Result<String, ServerFnError> {
    let fmt_datetime = |dt: DateTime<Utc>| {
        let local = convert_utc_to_local(dt);
        local.format("%Y-%m-%d %H:%M").to_string()
    };

    let redux = ssr::app_state().get_state();
    if let Some(backup) = redux.backups(&id) {
        if backup.is_paused() {
            Ok(String::from("paused"))
        } else if backup.had_error() {
            let msg = backup.error_message().unwrap();
            Ok(format!("error: {}", msg))
        } else if let Some(et) = backup.end_time() {
            Ok(format!("finished at {}", fmt_datetime(et)))
        } else {
            let progress = if backup.changed_files() == 0 {
                String::from("scanning... ")
            } else {
                let u = backup.files_uploaded();
                let c = backup.changed_files();
                format!("{u} of {c} files, ")
            };
            let st = backup.start_time();
            Ok(format!("{progress} started at {}", fmt_datetime(st)))
        }
    } else {
        // application state will be empty after application start and before
        // the fist backup has started; use the dataset and its snapshot to
        // produce the last known status
        let dataset = get_dataset_0(id.clone())
            .await?
            .ok_or_else(|| ServerFnErrorErr::ServerError(format!("missing dataset {}", id)))?;
        if let Some(digest) = dataset.snapshot {
            let snapshot = get_snapshot_0(digest.clone()).await?.ok_or_else(|| {
                ServerFnErrorErr::ServerError(format!("missing snapshot {}", digest))
            })?;
            if let Some(et) = snapshot.end_time {
                Ok(format!("finished at {}", fmt_datetime(et)))
            } else {
                Ok(format!("started at {}", fmt_datetime(snapshot.start_time)))
            }
        } else {
            Ok(String::from("no backup yet"))
        }
    }
}

/// Count the number of existing snapshots for a dataset and return the
/// date/time of the oldest and latest snapshots.
#[leptos::server]
async fn count_snapshots(id: String) -> Result<SnapshotCount, ServerFnError> {
    use crate::domain::usecases::count_snapshots::{CountSnapshots, Params};
    use crate::domain::usecases::UseCase;
    use leptos::server_fn::error::ServerFnErrorErr;

    let repo = ssr::db()?;
    let usecase = CountSnapshots::new(Box::new(repo));
    let params = Params::new(id.clone());
    let counts = usecase
        .call(params)
        .map_err(|e| ServerFnErrorErr::ServerError(e.to_string()))?;
    Ok(counts)
}

/// Retrieve a specific snapshot record.
#[leptos::server(name = GetSnapshot, prefix = "/api", input = server_fn::codec::Cbor)]
pub async fn get_snapshot(digest: Option<Checksum>) -> Result<Option<Snapshot>, ServerFnError> {
    use crate::domain::usecases::get_snapshot::{GetSnapshot, Params};
    use crate::domain::usecases::UseCase;
    use leptos::server_fn::error::ServerFnErrorErr;

    if let Some(digest) = digest {
        let repo = ssr::db()?;
        let usecase = GetSnapshot::new(Box::new(repo));
        let params = Params::new(digest);
        let snapshot = usecase
            .call(params)
            .map_err(|e| ServerFnErrorErr::ServerError(e.to_string()))?;
        return Ok(snapshot);
    }
    Ok(None)
}
