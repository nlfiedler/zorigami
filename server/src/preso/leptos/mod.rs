//
// Copyright (c) 2025 Nathan Fiedler
//
use crate::domain::entities::Dataset;
use leptos::prelude::*;
use leptos_meta::*;
use leptos_router::components::*;
use leptos_router::path;

mod home;
mod nav;

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
    use crate::domain::sources::EntityDataSource;
    use server_fn::error::ServerFnErrorErr;
    use server_fn::ServerFnError;
    use std::env;
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
        let path = env::var("DB_PATH").unwrap_or_else(|_| DEFAULT_DB_PATH.to_owned());
        PathBuf::from(path)
    });

    /// Construct a repository implementation for the database.
    pub fn db() -> Result<RecordRepositoryImpl, ServerFnError> {
        let source = EntityDataSourceImpl::new(DB_PATH.as_path())
            .map_err(|e| ServerFnErrorErr::WrappedServerError(e))?;
        let datasource: Arc<dyn EntityDataSource> = Arc::new(source);
        let repo = RecordRepositoryImpl::new(datasource);
        Ok(repo)
    }
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
        .map_err(|e| ServerFnErrorErr::WrappedServerError(e))?;
    Ok(datasets)
}
