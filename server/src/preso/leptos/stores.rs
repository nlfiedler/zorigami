//
// Copyright (c) 2025 Nathan Fiedler
//
use crate::domain::entities::{PackRetention, Store, StoreType};
use crate::preso::leptos::nav;
use leptos::html::{Div, Input, Select};
use leptos::prelude::*;
use leptos_router::components::Outlet;
use leptos_router::hooks::use_params_map;
use leptos_use::on_click_outside;
use std::collections::HashMap;

/// Retrieve all pack stores.
#[leptos::server]
pub async fn stores() -> Result<Vec<Store>, ServerFnError> {
    use crate::domain::usecases::get_stores::GetStores;
    use crate::domain::usecases::{NoParams, UseCase};
    use leptos::server_fn::error::ServerFnErrorErr;

    let repo = super::ssr::db()?;
    let usecase = GetStores::new(Box::new(repo));
    let params = NoParams {};
    let stores: Vec<Store> = usecase
        .call(params)
        .map_err(|e| ServerFnErrorErr::ServerError(e.to_string()))?;
    Ok(stores)
}

/// Retrieve one pack store.
#[leptos::server]
pub async fn get_store(id: Option<String>) -> Result<Option<Store>, ServerFnError> {
    use crate::domain::usecases::get_stores::GetStores;
    use crate::domain::usecases::{NoParams, UseCase};
    use leptos::server_fn::error::ServerFnErrorErr;

    // calling get_stores() to get one store is hardly efficient but for now it
    // is not bad enough to add another use case
    if let Some(id) = id {
        let repo = super::ssr::db()?;
        let usecase = GetStores::new(Box::new(repo));
        let params = NoParams {};
        let stores: Vec<Store> = usecase
            .call(params)
            .map_err(|e| ServerFnErrorErr::ServerError(e.to_string()))?;
        for store in stores.into_iter() {
            if store.id == id {
                return Ok(Some(store));
            }
        }
    }
    Ok(None)
}

/// Create a dummy store for the given type.
fn create_dummy_store(store_type: StoreType) -> Store {
    let mut props: HashMap<String, String> = HashMap::new();
    match store_type {
        StoreType::AMAZON => {
            props.insert("region".into(), "us-east-1".into());
            props.insert("access_key".into(), "AKIAIOSFODNN7EXAMPLE".into());
            props.insert(
                "secret_key".into(),
                "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
            );
            props.insert("storage".into(), "STANDARD_IA".into());
            return Store {
                id: "auto-generated".into(),
                store_type,
                label: store_type.to_string(),
                properties: props,
                retention: PackRetention::ALL,
            };
        }
        StoreType::AZURE => {
            props.insert("account".into(), "my-storage".into());
            props.insert("access_key".into(), "AKIAIOSFODNN7EXAMPLE".into());
            props.insert("access_tier".into(), "Cool".into());
            props.insert("custom_uri".into(), String::new());
            return Store {
                id: "auto-generated".into(),
                store_type,
                label: store_type.to_string(),
                properties: props,
                retention: PackRetention::ALL,
            };
        }
        StoreType::GOOGLE => {
            props.insert(
                "credentials".into(),
                "/Users/charlie/credentials.json".into(),
            );
            props.insert("project".into(), "white-sunspot-12345".into());
            props.insert("region".into(), "us-west1".into());
            props.insert("storage".into(), "NEARLINE".into());
            return Store {
                id: "auto-generated".into(),
                store_type,
                label: store_type.to_string(),
                properties: props,
                retention: PackRetention::ALL,
            };
        }
        StoreType::LOCAL => {
            props.insert("basepath".into(), ".".into());
            return Store {
                id: "auto-generated".into(),
                store_type,
                label: store_type.to_string(),
                properties: props,
                retention: PackRetention::ALL,
            };
        }
        StoreType::MINIO => {
            props.insert("region".into(), "us-west-1".into());
            props.insert("endpoint".into(), "http://192.168.1.1:9000".into());
            props.insert("access_key".into(), "AKIAIOSFODNN7EXAMPLE".into());
            props.insert(
                "secret_key".into(),
                "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
            );
            return Store {
                id: "auto-generated".into(),
                store_type,
                label: store_type.to_string(),
                properties: props,
                retention: PackRetention::ALL,
            };
        }
        StoreType::SFTP => {
            props.insert("remote_addr".into(), "127.0.0.1:22".into());
            props.insert("username".into(), "charlie".into());
            props.insert("password".into(), "secret123".into());
            props.insert("basepath".into(), ".".into());
            return Store {
                id: "auto-generated".into(),
                store_type,
                label: store_type.to_string(),
                properties: props,
                retention: PackRetention::ALL,
            };
        }
    }
}

///
/// Create a new pack store with the given values, returning its identifier.
///
#[leptos::server]
async fn create_store(store: Store) -> Result<String, ServerFnError> {
    use crate::domain::usecases::new_store::{NewStore, Params};
    use crate::domain::usecases::UseCase;
    use server_fn::error::ServerFnErrorErr;

    let repo = super::ssr::db()?;
    let usecase = NewStore::new(Box::new(repo));
    let params: Params = Params::from(store);
    let result = usecase
        .call(params)
        .map_err(|e| ServerFnErrorErr::ServerError(e.to_string()))?;
    Ok(result.id)
}

///
/// Test the pack store connection with the given values.
///
#[leptos::server]
async fn test_store(store: Store) -> Result<(), ServerFnError> {
    use crate::domain::usecases::test_store::{Params, TestStore};
    use crate::domain::usecases::UseCase;
    use server_fn::error::ServerFnErrorErr;

    let repo = super::ssr::db()?;
    let usecase = TestStore::new(Box::new(repo));
    let params: Params = Params::from(store);
    let result = usecase.call(params);
    if let Err(ref err) = result {
        log::error!("test_store failed: {}", err);
    }
    result.map_err(|e| ServerFnErrorErr::ServerError(e.to_string()))?;
    Ok(())
}

///
/// Update the pack store with the given values.
///
#[leptos::server]
async fn update_store(store: Store) -> Result<(), ServerFnError> {
    use crate::domain::usecases::update_store::{Params, UpdateStore};
    use crate::domain::usecases::UseCase;
    use server_fn::error::ServerFnErrorErr;

    let repo = super::ssr::db()?;
    let usecase = UpdateStore::new(Box::new(repo));
    let params: Params = Params::from(store);
    usecase
        .call(params)
        .map_err(|e| ServerFnErrorErr::ServerError(e.to_string()))?;
    Ok(())
}

///
/// Delete the pack store with the given identifier.
///
#[leptos::server]
async fn delete_store(id: String) -> Result<(), ServerFnError> {
    use crate::domain::usecases::delete_store::{DeleteStore, Params};
    use crate::domain::usecases::UseCase;
    use server_fn::error::ServerFnErrorErr;

    let repo = super::ssr::db()?;
    let usecase = DeleteStore::new(Box::new(repo));
    let params: Params = Params::new(id);
    usecase
        .call(params)
        .map_err(|e| ServerFnErrorErr::ServerError(e.to_string()))?;
    Ok(())
}

#[component]
pub fn StoresPage() -> impl IntoView {
    let location = leptos_router::hooks::use_location();
    let stores_resource = Resource::new(
        // use location memo as a hack to refetch the stores whenever a new
        // store is added or removed, which results in the path changing
        move || location.pathname.get(),
        |_| async move {
            // sort the stores by identifier for consistent ordering
            let mut results = stores().await;
            if let Ok(data) = results.as_mut() {
                data.sort_by(|a, b| a.id.cmp(&b.id));
            }
            results
        },
    );
    let dropdown_open = RwSignal::new(false);
    let dropdown_ref: NodeRef<Div> = NodeRef::new();
    let _ = on_click_outside(dropdown_ref, move |_| dropdown_open.set(false));
    let create_store = Action::new(move |store_type: &StoreType| {
        let store_type = *store_type;
        async move {
            let dummy = create_dummy_store(store_type);
            match create_store(dummy).await {
                Ok(id) => {
                    stores_resource.refetch();
                    let navigate = leptos_router::hooks::use_navigate();
                    let url = format!("/stores/{}", id);
                    navigate(&url, Default::default());
                }
                Err(err) => {
                    log::error!("bulk edit failed: {err:#?}");
                }
            }
        }
    });
    let store_types = StoredValue::new(vec![
        ("Amazon", StoreType::AMAZON),
        ("Azure", StoreType::AZURE),
        ("Google", StoreType::GOOGLE),
        ("Local", StoreType::LOCAL),
        ("MinIO", StoreType::MINIO),
        ("SFTP", StoreType::SFTP),
    ]);

    view! {
        <nav::NavBar />

        <div class="container">
            <nav class="level">
                <div class="level-left">
                    <div class="level-item">
                        <div
                            class="dropdown"
                            class:is-active=move || dropdown_open.get()
                            node_ref=dropdown_ref
                        >
                            <div class="dropdown-trigger">
                                <button
                                    class="button"
                                    on:click=move |_| { dropdown_open.update(|v| { *v = !*v }) }
                                    aria-haspopup="true"
                                    aria-controls="dropdown-menu"
                                >
                                    <span class="icon">
                                        <i class="fa-solid fa-circle-plus"></i>
                                    </span>
                                    <span>New Store</span>
                                </button>
                            </div>
                            <div class="dropdown-menu" id="dropdown-menu" role="menu">
                                <div class="dropdown-content">
                                    <For each=move || store_types.get_value() key=|s| s.0 let:store>
                                        <a
                                            class="dropdown-item"
                                            on:click=move |_| {
                                                create_store.dispatch(store.1);
                                                dropdown_open.set(false)
                                            }
                                        >
                                            {store.0}
                                        </a>
                                    </For>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="level-right">
                    <Transition fallback=move || {
                        view! { "Loading..." }
                    }>
                        {move || {
                            stores_resource
                                .get()
                                .map(|result| match result {
                                    Err(err) => {
                                        view! { <span>{move || format!("Error: {}", err)}</span> }
                                            .into_any()
                                    }
                                    Ok(stores) => {
                                        let stored = StoredValue::new(stores);
                                        view! {
                                            <For
                                                each=move || {
                                                    stored.get_value().into_iter().map(|s| StoredValue::new(s))
                                                }
                                                key=|s| s.get_value().id
                                                let:store
                                            >
                                                <div class="level-item">
                                                    <a href=move || {
                                                        format!("/stores/{}", store.get_value().id)
                                                    }>
                                                        <button class="button">{store.get_value().label}</button>
                                                    </a>
                                                </div>
                                            </For>
                                        }
                                            .into_any()
                                    }
                                })
                        }}
                    </Transition>
                </div>
            </nav>
            <Transition fallback=move || {
                view! { "Loading..." }
            }>
                {move || {
                    stores_resource
                        .get()
                        .map(|result| match result {
                            Err(err) => {
                                view! { <span>{move || format!("Error: {}", err)}</span> }
                                    .into_any()
                            }
                            Ok(stores) => {
                                if stores.is_empty() {
                                    view! {
                                        <div class="container">
                                            <p class="m-2 title is-5">No pack stores.</p>
                                            <p class="subtitle is-5">
                                                Use the <strong class="mx-1">New Store</strong>
                                                button to create a pack store.
                                            </p>
                                        </div>
                                    }
                                        .into_any()
                                } else {
                                    view! { <Outlet /> }.into_any()
                                }
                            }
                        })
                }}
            </Transition>
        </div>
    }
}

#[component]
pub fn StoreDetails() -> impl IntoView {
    let params = use_params_map();
    let store_resource = Resource::new(
        move || params.with(|params| params.get("id")),
        |id| async move { get_store(id).await },
    );
    let values_changed = Action::new(move |_input: &()| {
        async move {
            // for now, just cause the page to reload to reflect whatever changes
            // were made, including deleting or updating a store
            let navigate = leptos_router::hooks::use_navigate();
            navigate("/stores", Default::default());
        }
    });

    view! {
        <Transition fallback=move || {
            view! { "Loading..." }
        }>
            {move || {
                store_resource
                    .get()
                    .map(|result| match result {
                        Err(err) => {
                            view! { <span>{move || format!("Error: {}", err)}</span> }.into_any()
                        }
                        Ok(details) => {
                            if let Some(store) = details {
                                match store.store_type {
                                    StoreType::AMAZON => {
                                        view! {
                                            <AmazonStoreForm
                                                store
                                                changed=move || {
                                                    values_changed.dispatch(());
                                                }
                                            />
                                        }
                                            .into_any()
                                    }
                                    StoreType::AZURE => {
                                        view! {
                                            <AzureStoreForm
                                                store
                                                changed=move || {
                                                    values_changed.dispatch(());
                                                }
                                            />
                                        }
                                            .into_any()
                                    }
                                    StoreType::GOOGLE => {
                                        view! {
                                            <GoogleStoreForm
                                                store
                                                changed=move || {
                                                    values_changed.dispatch(());
                                                }
                                            />
                                        }
                                            .into_any()
                                    }
                                    StoreType::LOCAL => {
                                        view! {
                                            <LocalStoreForm
                                                store
                                                changed=move || {
                                                    values_changed.dispatch(());
                                                }
                                            />
                                        }
                                            .into_any()
                                    }
                                    StoreType::MINIO => {
                                        view! {
                                            <MinioStoreForm
                                                store
                                                changed=move || {
                                                    values_changed.dispatch(());
                                                }
                                            />
                                        }
                                            .into_any()
                                    }
                                    StoreType::SFTP => {
                                        view! {
                                            <SftpStoreForm
                                                store
                                                changed=move || {
                                                    values_changed.dispatch(());
                                                }
                                            />
                                        }
                                            .into_any()
                                    }
                                }
                            } else {
                                view! {
                                    <div class="m-4">
                                        <p class="subtitle is-5">Error: no store?</p>
                                    </div>
                                }
                                    .into_any()
                            }
                                .into_any()
                        }
                    })
            }}
        </Transition>
    }
}

#[component]
fn DeleteStoreButton<E>(store: StoredValue<Store>, deleted: E) -> impl IntoView
where
    E: Fn() + Copy + 'static + Send,
{
    let delete_action = Action::new_local(move |_input: &()| {
        let result = delete_store(store.get_value().id);
        deleted();
        result
    });

    view! {
        <button
            class="button is-danger"
            on:click=move |_| {
                delete_action.dispatch(());
            }
        >
            <span class="icon">
                <i class="fa-solid fa-trash-can"></i>
            </span>
            <span>Delete</span>
        </button>
    }
}

#[component]
fn TestStoreButton<F>(build_store: F, set_test_error_msg: WriteSignal<String>) -> impl IntoView
where
    F: Fn() -> Store + 'static,
{
    let test_action = Action::new_local(move |_input: &()| {
        let store = build_store();
        test_store(store)
    });
    Effect::new(move |_| {
        // cannot read test_store() result inside action and set the signal at
        // the same time (Fn captures environment)
        if let Some(Err(err)) = test_action.value().get() {
            log::error!("error: {}", err.to_string());
            set_test_error_msg.set(err.to_string());
        } else {
            set_test_error_msg.set(String::new());
        }
    });
    let test_pending = test_action.pending();

    view! {
        <Show
            when=move || test_pending.get()
            fallback=move || {
                view! {
                    <Show
                        when=move || {
                            test_action
                                .value()
                                .get()
                                .map(|r| r.map(|_| true).unwrap_or(false))
                                .unwrap_or(false)
                        }
                        fallback=move || {
                            view! {
                                <button
                                    class="button"
                                    on:click=move |_| {
                                        test_action.dispatch(());
                                    }
                                >
                                    <span class="icon">
                                        <i class="fa-solid fa-satellite-dish"></i>
                                    </span>
                                    <span>Test</span>
                                </button>
                            }
                        }
                    >
                        <button
                            class="button is-success"
                            on:click=move |_| {
                                test_action.dispatch(());
                            }
                        >
                            <span class="icon is-small">
                                <i class="fas fa-satellite-dish"></i>
                            </span>
                            <span>Test</span>
                        </button>
                    </Show>
                }
            }
        >
            <button class="button is-loading">Test</button>
        </Show>
    }
}

#[component]
fn SaveStoreButton<E, F>(build_store: F, changed: E) -> impl IntoView
where
    E: Fn() + Copy + 'static + Send,
    F: Fn() -> Store + 'static,
{
    let save_action = Action::new_local(move |_input: &()| {
        let store = build_store();
        let result = update_store(store);
        changed();
        result
    });
    let save_pending = save_action.pending();

    view! {
        <Show
            when=move || save_pending.get()
            fallback=move || {
                view! {
                    <Show
                        when=move || {
                            save_action
                                .value()
                                .get()
                                .map(|r| r.map(|_| true).unwrap_or(false))
                                .unwrap_or(false)
                        }
                        fallback=move || {
                            view! {
                                <button
                                    class="button is-primary"
                                    on:click=move |_| {
                                        save_action.dispatch(());
                                    }
                                >
                                    <span class="icon">
                                        <i class="fa-solid fa-floppy-disk"></i>
                                    </span>
                                    <span>Save</span>
                                </button>
                            }
                        }
                    >
                        <button
                            class="button is-success"
                            on:click=move |_| {
                                save_action.dispatch(());
                            }
                        >
                            <span class="icon is-small">
                                <i class="fas fa-check"></i>
                            </span>
                            <span>Save</span>
                        </button>
                    </Show>
                }
            }
        >
            <button class="button is-loading">Save</button>
        </Show>
    }
}

#[component]
fn LocalStoreForm<E>(store: Store, changed: E) -> impl IntoView
where
    E: Fn() + Copy + 'static + Send,
{
    let label_input_ref: NodeRef<Input> = NodeRef::new();
    let basepath_input_ref: NodeRef<Input> = NodeRef::new();
    let basepath: String = if let Some(value) = store.properties.get("basepath") {
        value.to_owned()
    } else {
        String::new()
    };
    let store = StoredValue::new(store);
    let build_store = move || {
        let new_label = label_input_ref.get().unwrap().value();
        let new_basepath = basepath_input_ref.get().unwrap().value();
        let mut props: HashMap<String, String> = HashMap::new();
        props.insert("basepath".into(), new_basepath);
        Store {
            id: store.get_value().id.clone(),
            store_type: store.get_value().store_type.clone(),
            label: new_label,
            properties: props,
            retention: PackRetention::ALL,
        }
    };
    let (test_error_msg, set_test_error_msg) = signal(String::new());

    view! {
        <h2 class="m-4 title">Attached disk</h2>
        <nav class="m-4 level">
            <div class="level-left">
                <div class="level-item">
                    <DeleteStoreButton store deleted=changed />
                </div>
            </div>
            <div class="level-right">
                <div class="level-item">
                    <TestStoreButton build_store set_test_error_msg />
                </div>
                <div class="level-item">
                    <SaveStoreButton build_store changed />
                </div>
            </div>
        </nav>
        <div
            class="notification is-warning"
            class:is-hidden=move || test_error_msg.get().is_empty()
        >
            <button class="delete" on:click=move |_| set_test_error_msg.set(String::new())></button>
            {move || format!("{}", test_error_msg.get())}
        </div>
        <div class="m-4">
            <div class="mb-2 field is-horizontal">
                <div class="field-label is-normal">
                    <label class="label" for="label-input">
                        Label
                    </label>
                </div>
                <div class="field-body">
                    <div class="field">
                        <p class="control is-expanded has-icons-left">
                            <input
                                class="input"
                                type="text"
                                id="label-input"
                                node_ref=label_input_ref
                                placeholder="Descriptive label for the pack store."
                                value=store.get_value().label
                            />
                            <span class="icon is-small is-left">
                                <i class="fa-solid fa-quote-left"></i>
                            </span>
                        </p>
                    </div>
                </div>
            </div>

            <div class="field is-horizontal">
                <div class="field-label is-normal">
                    <label class="label" for="basepath-input">
                        Base Path
                    </label>
                </div>
                <div class="field-body">
                    <div class="field">
                        <p class="control is-expanded has-icons-left">
                            <input
                                class="input"
                                type="text"
                                id="basepath-input"
                                node_ref=basepath_input_ref
                                placeholder="Path to the local storage."
                                value=basepath
                            />
                            <span class="icon is-small is-left">
                                <i class="fa-solid fa-folder"></i>
                            </span>
                        </p>
                    </div>
                </div>
            </div>
        </div>
    }
}

#[component]
fn AmazonStoreForm<E>(store: Store, changed: E) -> impl IntoView
where
    E: Fn() + Copy + 'static + Send,
{
    let label_input_ref: NodeRef<Input> = NodeRef::new();
    let region_input_ref: NodeRef<Input> = NodeRef::new();
    let access_input_ref: NodeRef<Input> = NodeRef::new();
    let secret_input_ref: NodeRef<Input> = NodeRef::new();
    let class_input_ref: NodeRef<Select> = NodeRef::new();
    let region: String = if let Some(value) = store.properties.get("region") {
        value.to_owned()
    } else {
        String::new()
    };
    let access_key: String = if let Some(value) = store.properties.get("access_key") {
        value.to_owned()
    } else {
        String::new()
    };
    let secret_key: String = if let Some(value) = store.properties.get("secret_key") {
        value.to_owned()
    } else {
        String::new()
    };
    let storage = if let Some(value) = store.properties.get("storage") {
        StoredValue::new(value.to_owned())
    } else {
        StoredValue::new(String::new())
    };
    let store = StoredValue::new(store);
    let build_store = move || {
        let new_label = label_input_ref.get().unwrap().value();
        let new_region = region_input_ref.get().unwrap().value();
        let new_access = access_input_ref.get().unwrap().value();
        let new_secret = secret_input_ref.get().unwrap().value();
        let new_class = class_input_ref.get().unwrap().value();
        let mut props: HashMap<String, String> = HashMap::new();
        props.insert("region".into(), new_region);
        props.insert("access_key".into(), new_access);
        props.insert("secret_key".into(), new_secret);
        props.insert("storage".into(), new_class);
        Store {
            id: store.get_value().id.clone(),
            store_type: store.get_value().store_type.clone(),
            label: new_label,
            properties: props,
            retention: PackRetention::ALL,
        }
    };
    let (test_error_msg, set_test_error_msg) = signal(String::new());
    let secret_visible = RwSignal::new(false);

    view! {
        <h2 class="m-4 title">Amazon S3</h2>
        <nav class="m-4 level">
            <div class="level-left">
                <div class="level-item">
                    <DeleteStoreButton store deleted=changed />
                </div>
            </div>
            <div class="level-right">
                <div class="level-item">
                    <TestStoreButton build_store set_test_error_msg />
                </div>
                <div class="level-item">
                    <SaveStoreButton build_store changed />
                </div>
            </div>
        </nav>
        <div
            class="notification is-warning"
            class:is-hidden=move || test_error_msg.get().is_empty()
        >
            <button class="delete" on:click=move |_| set_test_error_msg.set(String::new())></button>
            {move || format!("{}", test_error_msg.get())}
        </div>
        <div class="m-4">
            <div class="mb-2 field is-horizontal">
                <div class="field-label is-normal">
                    <label class="label" for="label-input">
                        Label
                    </label>
                </div>
                <div class="field-body">
                    <div class="field">
                        <p class="control is-expanded has-icons-left">
                            <input
                                class="input"
                                type="text"
                                id="label-input"
                                node_ref=label_input_ref
                                placeholder="Descriptive label for the pack store."
                                value=store.get_value().label
                            />
                            <span class="icon is-small is-left">
                                <i class="fa-solid fa-quote-left"></i>
                            </span>
                        </p>
                    </div>
                </div>
            </div>

            <div class="mb-2 field is-horizontal">
                <div class="field-label is-normal">
                    <label class="label" for="region-input">
                        Region
                    </label>
                </div>
                <div class="field-body">
                    <div class="field">
                        <p class="control is-expanded has-icons-left">
                            <input
                                class="input"
                                type="text"
                                id="region-input"
                                node_ref=region_input_ref
                                placeholder="Geographic region or availability zone."
                                value=region
                            />
                            <span class="icon is-small is-left">
                                <i class="fa-solid fa-globe"></i>
                            </span>
                        </p>
                    </div>
                </div>
            </div>

            <div class="mb-2 field is-horizontal">
                <div class="field-label is-normal">
                    <label class="label" for="access-input">
                        Access Key
                    </label>
                </div>
                <div class="field-body">
                    <div class="field">
                        <p class="control is-expanded has-icons-left">
                            <input
                                class="input"
                                type="text"
                                id="access-input"
                                node_ref=access_input_ref
                                placeholder="Access key identifier."
                                value=access_key
                            />
                            <span class="icon is-small is-left">
                                <i class="fa-solid fa-circle-info"></i>
                            </span>
                        </p>
                    </div>
                </div>
            </div>

            <div class="mb-2 field is-horizontal">
                <div class="field-label is-normal">
                    <label class="label" for="secret-input">
                        Secret Key
                    </label>
                </div>
                <div class="field-body">
                    <div class="field has-addons">
                        <div class="control is-expanded has-icons-left">
                            <input
                                class="input"
                                type=move || if secret_visible.get() { "text" } else { "password" }
                                id="secret-input"
                                node_ref=secret_input_ref
                                placeholder="Secret access key."
                                value=secret_key
                            />
                            <span class="icon is-small is-left">
                                <i class="fa-solid fa-key"></i>
                            </span>
                        </div>
                        <div class="control">
                            <button
                                class="button"
                                on:click=move |_| secret_visible.update(|v| { *v = !*v })
                            >
                                <span class="icon is-small">
                                    <i class=move || {
                                        if secret_visible.get() {
                                            "fas fa-eye-slash"
                                        } else {
                                            "fas fa-eye"
                                        }
                                    }></i>
                                </span>
                            </button>
                        </div>
                    </div>
                </div>
            </div>

            <div class="field is-horizontal">
                <div class="field-label is-normal">
                    <label class="label" for="storage-input">
                        Storage Class
                    </label>
                </div>
                <div class="field-body">
                    <div class="field is-narrow">
                        <div class="control has-icons-left">
                            <span class="select is-fullwidth">
                                <select id="storage-input" node_ref=class_input_ref>
                                    <option selected=move || {
                                        storage.get_value().eq_ignore_ascii_case("standard")
                                    }>STANDARD</option>
                                    <option selected=move || {
                                        storage.get_value().eq_ignore_ascii_case("standard_ia")
                                    }>STANDARD_IA</option>
                                    <option selected=move || {
                                        storage.get_value().eq_ignore_ascii_case("glacier_ir")
                                    }>GLACIER_IR</option>
                                </select>
                            </span>
                            <span class="icon is-small is-left">
                                <i class="fas fa-hard-drive"></i>
                            </span>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    }
}

#[component]
fn AzureStoreForm<E>(store: Store, changed: E) -> impl IntoView
where
    E: Fn() + Copy + 'static + Send,
{
    let label_input_ref: NodeRef<Input> = NodeRef::new();
    let account_input_ref: NodeRef<Input> = NodeRef::new();
    let access_input_ref: NodeRef<Input> = NodeRef::new();
    let tier_input_ref: NodeRef<Select> = NodeRef::new();
    let uri_input_ref: NodeRef<Input> = NodeRef::new();
    let account: String = if let Some(value) = store.properties.get("account") {
        value.to_owned()
    } else {
        String::new()
    };
    let access_key: String = if let Some(value) = store.properties.get("access_key") {
        value.to_owned()
    } else {
        String::new()
    };
    let access_tier = if let Some(value) = store.properties.get("access_tier") {
        StoredValue::new(value.to_owned())
    } else {
        StoredValue::new(String::new())
    };
    let custom_uri: String = if let Some(value) = store.properties.get("custom_uri") {
        value.to_owned()
    } else {
        String::new()
    };
    let store = StoredValue::new(store);
    let build_store = move || {
        let new_label = label_input_ref.get().unwrap().value();
        let new_account = account_input_ref.get().unwrap().value();
        let new_access = access_input_ref.get().unwrap().value();
        let new_tier = tier_input_ref.get().unwrap().value();
        let new_uri = uri_input_ref.get().unwrap().value();
        let mut props: HashMap<String, String> = HashMap::new();
        props.insert("account".into(), new_account);
        props.insert("access_key".into(), new_access);
        props.insert("access_tier".into(), new_tier);
        if new_uri.len() > 0 {
            // only set the custom_uri if the value is non-empty
            props.insert("custom_uri".into(), new_uri);
        }
        Store {
            id: store.get_value().id.clone(),
            store_type: store.get_value().store_type.clone(),
            label: new_label,
            properties: props,
            retention: PackRetention::ALL,
        }
    };
    let (test_error_msg, set_test_error_msg) = signal(String::new());
    let secret_visible = RwSignal::new(false);

    view! {
        <h2 class="m-4 title">Azure Blob Storage</h2>
        <nav class="m-4 level">
            <div class="level-left">
                <div class="level-item">
                    <DeleteStoreButton store deleted=changed />
                </div>
            </div>
            <div class="level-right">
                <div class="level-item">
                    <TestStoreButton build_store set_test_error_msg />
                </div>
                <div class="level-item">
                    <SaveStoreButton build_store changed />
                </div>
            </div>
        </nav>
        <div
            class="notification is-warning"
            class:is-hidden=move || test_error_msg.get().is_empty()
        >
            <button class="delete" on:click=move |_| set_test_error_msg.set(String::new())></button>
            {move || format!("{}", test_error_msg.get())}
        </div>
        <div class="m-4">
            <div class="mb-2 field is-horizontal">
                <div class="field-label is-normal">
                    <label class="label" for="label-input">
                        Label
                    </label>
                </div>
                <div class="field-body">
                    <div class="field">
                        <p class="control is-expanded has-icons-left">
                            <input
                                class="input"
                                type="text"
                                id="label-input"
                                node_ref=label_input_ref
                                placeholder="Descriptive label for the pack store."
                                value=store.get_value().label
                            />
                            <span class="icon is-small is-left">
                                <i class="fa-solid fa-quote-left"></i>
                            </span>
                        </p>
                    </div>
                </div>
            </div>

            <div class="mb-2 field is-horizontal">
                <div class="field-label is-normal">
                    <label class="label" for="account-input">
                        Account Name
                    </label>
                </div>
                <div class="field-body">
                    <div class="field">
                        <p class="control is-expanded has-icons-left">
                            <input
                                class="input"
                                type="text"
                                id="account-input"
                                node_ref=account_input_ref
                                placeholder="Name of the storage account."
                                value=account
                            />
                            <span class="icon is-small is-left">
                                <i class="fa-solid fa-cloud"></i>
                            </span>
                        </p>
                    </div>
                </div>
            </div>

            <div class="mb-2 field is-horizontal">
                <div class="field-label is-normal">
                    <label class="label" for="access-input">
                        Access Key
                    </label>
                </div>
                <div class="field-body">
                    <div class="field has-addons">
                        <div class="control is-expanded has-icons-left">
                            <input
                                class="input"
                                type=move || if secret_visible.get() { "text" } else { "password" }
                                id="access-input"
                                node_ref=access_input_ref
                                placeholder="Access key."
                                value=access_key
                            />
                            <span class="icon is-small is-left">
                                <i class="fa-solid fa-key"></i>
                            </span>
                        </div>
                        <div class="control">
                            <button
                                class="button"
                                on:click=move |_| secret_visible.update(|v| { *v = !*v })
                            >
                                <span class="icon is-small">
                                    <i class=move || {
                                        if secret_visible.get() {
                                            "fas fa-eye-slash"
                                        } else {
                                            "fas fa-eye"
                                        }
                                    }></i>
                                </span>
                            </button>
                        </div>
                    </div>
                </div>
            </div>

            <div class="mb-2 field is-horizontal">
                <div class="field-label is-normal">
                    <label class="label" for="tier-input">
                        Access Tier
                    </label>
                </div>
                <div class="field-body">
                    <div class="field is-narrow">
                        <div class="control has-icons-left">
                            <span class="select is-fullwidth">
                                <select id="tier-input" node_ref=tier_input_ref>
                                    <option selected=move || {
                                        access_tier.get_value().eq_ignore_ascii_case("hot")
                                    }>Hot</option>
                                    <option selected=move || {
                                        access_tier.get_value().eq_ignore_ascii_case("cool")
                                    }>Cool</option>
                                </select>
                            </span>
                            <span class="icon is-small is-left">
                                <i class="fas fa-hard-drive"></i>
                            </span>
                        </div>
                    </div>
                </div>
            </div>

            <div class="field is-horizontal">
                <div class="field-label is-normal">
                    <label class="label" for="uri-input">
                        Custom URI
                    </label>
                </div>
                <div class="field-body">
                    <div class="field">
                        <p class="control is-expanded has-icons-left">
                            <input
                                class="input"
                                type="text"
                                id="uri-input"
                                node_ref=uri_input_ref
                                placeholder="Custom URI."
                                value=custom_uri
                            />
                            <span class="icon is-small is-left">
                                <i class="fa-solid fa-link"></i>
                            </span>
                        </p>
                    </div>
                </div>
            </div>
        </div>
    }
}

#[component]
fn GoogleStoreForm<E>(store: Store, changed: E) -> impl IntoView
where
    E: Fn() + Copy + 'static + Send,
{
    let label_input_ref: NodeRef<Input> = NodeRef::new();
    let creds_input_ref: NodeRef<Input> = NodeRef::new();
    let project_input_ref: NodeRef<Input> = NodeRef::new();
    let region_input_ref: NodeRef<Input> = NodeRef::new();
    let class_input_ref: NodeRef<Select> = NodeRef::new();
    let credentials: String = if let Some(value) = store.properties.get("credentials") {
        value.to_owned()
    } else {
        String::new()
    };
    let project_id: String = if let Some(value) = store.properties.get("project") {
        value.to_owned()
    } else {
        String::new()
    };
    let region: String = if let Some(value) = store.properties.get("region") {
        value.to_owned()
    } else {
        String::new()
    };
    let storage = if let Some(value) = store.properties.get("storage") {
        StoredValue::new(value.to_owned())
    } else {
        StoredValue::new(String::new())
    };
    let store = StoredValue::new(store);
    let build_store = move || {
        let new_label = label_input_ref.get().unwrap().value();
        let new_creds = creds_input_ref.get().unwrap().value();
        let new_project = project_input_ref.get().unwrap().value();
        let new_region = region_input_ref.get().unwrap().value();
        let new_class = class_input_ref.get().unwrap().value();
        let mut props: HashMap<String, String> = HashMap::new();
        props.insert("credentials".into(), new_creds);
        props.insert("project".into(), new_project);
        props.insert("region".into(), new_region);
        props.insert("storage".into(), new_class);
        Store {
            id: store.get_value().id.clone(),
            store_type: store.get_value().store_type.clone(),
            label: new_label,
            properties: props,
            retention: PackRetention::ALL,
        }
    };
    let (test_error_msg, set_test_error_msg) = signal(String::new());

    view! {
        <h2 class="m-4 title">Google Cloud Storage</h2>
        <nav class="m-4 level">
            <div class="level-left">
                <div class="level-item">
                    <DeleteStoreButton store deleted=changed />
                </div>
            </div>
            <div class="level-right">
                <div class="level-item">
                    <TestStoreButton build_store set_test_error_msg />
                </div>
                <div class="level-item">
                    <SaveStoreButton build_store changed />
                </div>
            </div>
        </nav>
        <div
            class="notification is-warning"
            class:is-hidden=move || test_error_msg.get().is_empty()
        >
            <button class="delete" on:click=move |_| set_test_error_msg.set(String::new())></button>
            {move || format!("{}", test_error_msg.get())}
        </div>
        <div class="m-4">
            <div class="mb-2 field is-horizontal">
                <div class="field-label is-normal">
                    <label class="label" for="label-input">
                        Label
                    </label>
                </div>
                <div class="field-body">
                    <div class="field">
                        <p class="control is-expanded has-icons-left">
                            <input
                                class="input"
                                type="text"
                                id="label-input"
                                node_ref=label_input_ref
                                placeholder="Descriptive label for the pack store."
                                value=store.get_value().label
                            />
                            <span class="icon is-small is-left">
                                <i class="fa-solid fa-quote-left"></i>
                            </span>
                        </p>
                    </div>
                </div>
            </div>

            <div class="mb-2 field is-horizontal">
                <div class="field-label is-normal">
                    <label class="label" for="creds-input">
                        Credentials File
                    </label>
                </div>
                <div class="field-body">
                    <div class="field">
                        <p class="control is-expanded has-icons-left">
                            <input
                                class="input"
                                type="text"
                                id="creds-input"
                                node_ref=creds_input_ref
                                placeholder="Path to JSON credentials file."
                                value=credentials
                            />
                            <span class="icon is-small is-left">
                                <i class="fa-solid fa-key"></i>
                            </span>
                        </p>
                    </div>
                </div>
            </div>

            <div class="mb-2 field is-horizontal">
                <div class="field-label is-normal">
                    <label class="label" for="project-input">
                        Project ID
                    </label>
                </div>
                <div class="field-body">
                    <div class="field">
                        <p class="control is-expanded has-icons-left">
                            <input
                                class="input"
                                type="text"
                                id="project-input"
                                node_ref=project_input_ref
                                placeholder="Project identifier."
                                value=project_id
                            />
                            <span class="icon is-small is-left">
                                <i class="fa-solid fa-cloud"></i>
                            </span>
                        </p>
                    </div>
                </div>
            </div>

            <div class="mb-2 field is-horizontal">
                <div class="field-label is-normal">
                    <label class="label" for="region-input">
                        Region
                    </label>
                </div>
                <div class="field-body">
                    <div class="field">
                        <p class="control is-expanded has-icons-left">
                            <input
                                class="input"
                                type="text"
                                id="region-input"
                                node_ref=region_input_ref
                                placeholder="Geographic region or availability zone."
                                value=region
                            />
                            <span class="icon is-small is-left">
                                <i class="fa-solid fa-globe"></i>
                            </span>
                        </p>
                    </div>
                </div>
            </div>

            <div class="field is-horizontal">
                <div class="field-label is-normal">
                    <label class="label" for="storage-input">
                        Storage Class
                    </label>
                </div>
                <div class="field-body">
                    <div class="field is-narrow">
                        <div class="control has-icons-left">
                            <span class="select is-fullwidth">
                                <select id="storage-input" node_ref=class_input_ref>
                                    <option selected=move || {
                                        storage.get_value().eq_ignore_ascii_case("standard")
                                    }>STANDARD</option>
                                    <option selected=move || {
                                        storage.get_value().eq_ignore_ascii_case("nearline")
                                    }>NEARLINE</option>
                                    <option selected=move || {
                                        storage.get_value().eq_ignore_ascii_case("coldline")
                                    }>COLDLINE</option>
                                </select>
                            </span>
                            <span class="icon is-small is-left">
                                <i class="fas fa-hard-drive"></i>
                            </span>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    }
}

#[component]
fn MinioStoreForm<E>(store: Store, changed: E) -> impl IntoView
where
    E: Fn() + Copy + 'static + Send,
{
    let label_input_ref: NodeRef<Input> = NodeRef::new();
    let region_input_ref: NodeRef<Input> = NodeRef::new();
    let endpoint_input_ref: NodeRef<Input> = NodeRef::new();
    let access_input_ref: NodeRef<Input> = NodeRef::new();
    let secret_input_ref: NodeRef<Input> = NodeRef::new();
    let region: String = if let Some(value) = store.properties.get("region") {
        value.to_owned()
    } else {
        String::new()
    };
    let endpoint: String = if let Some(value) = store.properties.get("endpoint") {
        value.to_owned()
    } else {
        String::new()
    };
    let access_key: String = if let Some(value) = store.properties.get("access_key") {
        value.to_owned()
    } else {
        String::new()
    };
    let secret_key: String = if let Some(value) = store.properties.get("secret_key") {
        value.to_owned()
    } else {
        String::new()
    };
    let store = StoredValue::new(store);
    let build_store = move || {
        let new_label = label_input_ref.get().unwrap().value();
        let new_region = region_input_ref.get().unwrap().value();
        let new_endpoint = endpoint_input_ref.get().unwrap().value();
        let new_access = access_input_ref.get().unwrap().value();
        let new_secret = secret_input_ref.get().unwrap().value();
        let mut props: HashMap<String, String> = HashMap::new();
        props.insert("region".into(), new_region);
        props.insert("endpoint".into(), new_endpoint);
        props.insert("access_key".into(), new_access);
        props.insert("secret_key".into(), new_secret);
        Store {
            id: store.get_value().id.clone(),
            store_type: store.get_value().store_type.clone(),
            label: new_label,
            properties: props,
            retention: PackRetention::ALL,
        }
    };
    let (test_error_msg, set_test_error_msg) = signal(String::new());
    let secret_visible = RwSignal::new(false);

    view! {
        <h2 class="m-4 title">MinIO Object Storage</h2>
        <nav class="m-4 level">
            <div class="level-left">
                <div class="level-item">
                    <DeleteStoreButton store deleted=changed />
                </div>
            </div>
            <div class="level-right">
                <div class="level-item">
                    <TestStoreButton build_store set_test_error_msg />
                </div>
                <div class="level-item">
                    <SaveStoreButton build_store changed />
                </div>
            </div>
        </nav>
        <div
            class="notification is-warning"
            class:is-hidden=move || test_error_msg.get().is_empty()
        >
            <button class="delete" on:click=move |_| set_test_error_msg.set(String::new())></button>
            {move || format!("{}", test_error_msg.get())}
        </div>
        <div class="m-4">
            <div class="mb-2 field is-horizontal">
                <div class="field-label is-normal">
                    <label class="label" for="label-input">
                        Label
                    </label>
                </div>
                <div class="field-body">
                    <div class="field">
                        <p class="control is-expanded has-icons-left">
                            <input
                                class="input"
                                type="text"
                                id="label-input"
                                node_ref=label_input_ref
                                placeholder="Descriptive label for the pack store."
                                value=store.get_value().label
                            />
                            <span class="icon is-small is-left">
                                <i class="fa-solid fa-quote-left"></i>
                            </span>
                        </p>
                    </div>
                </div>
            </div>

            <div class="mb-2 field is-horizontal">
                <div class="field-label is-normal">
                    <label class="label" for="region-input">
                        Region
                    </label>
                </div>
                <div class="field-body">
                    <div class="field">
                        <p class="control is-expanded has-icons-left">
                            <input
                                class="input"
                                type="text"
                                id="region-input"
                                node_ref=region_input_ref
                                placeholder="Geographic region or availability zone."
                                value=region
                            />
                            <span class="icon is-small is-left">
                                <i class="fa-solid fa-globe"></i>
                            </span>
                        </p>
                    </div>
                </div>
            </div>

            <div class="mb-2 field is-horizontal">
                <div class="field-label is-normal">
                    <label class="label" for="endpoint-input">
                        Endpoint
                    </label>
                </div>
                <div class="field-body">
                    <div class="field">
                        <p class="control is-expanded has-icons-left">
                            <input
                                class="input"
                                type="text"
                                id="endpoint-input"
                                node_ref=endpoint_input_ref
                                placeholder="Endpoint URL."
                                value=endpoint
                            />
                            <span class="icon is-small is-left">
                                <i class="fa-solid fa-key"></i>
                            </span>
                        </p>
                    </div>
                </div>
            </div>

            <div class="mb-2 field is-horizontal">
                <div class="field-label is-normal">
                    <label class="label" for="access-input">
                        Access Key
                    </label>
                </div>
                <div class="field-body">
                    <div class="field">
                        <p class="control is-expanded has-icons-left">
                            <input
                                class="input"
                                type="text"
                                id="access-input"
                                node_ref=access_input_ref
                                placeholder="Access key identifier."
                                value=access_key
                            />
                            <span class="icon is-small is-left">
                                <i class="fa-solid fa-circle-info"></i>
                            </span>
                        </p>
                    </div>
                </div>
            </div>

            <div class="field is-horizontal">
                <div class="field-label is-normal">
                    <label class="label" for="secret-input">
                        Secret Key
                    </label>
                </div>
                <div class="field-body">
                    <div class="field has-addons">
                        <div class="control is-expanded has-icons-left">
                            <input
                                class="input"
                                type=move || if secret_visible.get() { "text" } else { "password" }
                                id="secret-input"
                                node_ref=secret_input_ref
                                placeholder="Secret access key."
                                value=secret_key
                            />
                            <span class="icon is-small is-left">
                                <i class="fa-solid fa-key"></i>
                            </span>
                        </div>
                        <div class="control">
                            <button
                                class="button"
                                on:click=move |_| secret_visible.update(|v| { *v = !*v })
                            >
                                <span class="icon is-small">
                                    <i class=move || {
                                        if secret_visible.get() {
                                            "fas fa-eye-slash"
                                        } else {
                                            "fas fa-eye"
                                        }
                                    }></i>
                                </span>
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    }
}

#[component]
fn SftpStoreForm<E>(store: Store, changed: E) -> impl IntoView
where
    E: Fn() + Copy + 'static + Send,
{
    let label_input_ref: NodeRef<Input> = NodeRef::new();
    let addr_input_ref: NodeRef<Input> = NodeRef::new();
    let username_input_ref: NodeRef<Input> = NodeRef::new();
    let passwd_input_ref: NodeRef<Input> = NodeRef::new();
    let path_input_ref: NodeRef<Input> = NodeRef::new();
    let remote_addr: String = if let Some(value) = store.properties.get("remote_addr") {
        value.to_owned()
    } else {
        String::new()
    };
    let username: String = if let Some(value) = store.properties.get("username") {
        value.to_owned()
    } else {
        String::new()
    };
    let password: String = if let Some(value) = store.properties.get("password") {
        value.to_owned()
    } else {
        String::new()
    };
    let basepath: String = if let Some(value) = store.properties.get("basepath") {
        value.to_owned()
    } else {
        String::new()
    };
    let store = StoredValue::new(store);
    let build_store = move || {
        let new_label = label_input_ref.get().unwrap().value();
        let new_addr = addr_input_ref.get().unwrap().value();
        let new_username = username_input_ref.get().unwrap().value();
        let new_password = passwd_input_ref.get().unwrap().value();
        let new_path = path_input_ref.get().unwrap().value();
        let mut props: HashMap<String, String> = HashMap::new();
        props.insert("remote_addr".into(), new_addr);
        props.insert("username".into(), new_username);
        props.insert("password".into(), new_password);
        props.insert("basepath".into(), new_path);
        Store {
            id: store.get_value().id.clone(),
            store_type: store.get_value().store_type.clone(),
            label: new_label,
            properties: props,
            retention: PackRetention::ALL,
        }
    };
    let (test_error_msg, set_test_error_msg) = signal(String::new());
    let secret_visible = RwSignal::new(false);

    view! {
        <h2 class="m-4 title">Secure FTP</h2>
        <nav class="m-4 level">
            <div class="level-left">
                <div class="level-item">
                    <DeleteStoreButton store deleted=changed />
                </div>
            </div>
            <div class="level-right">
                <div class="level-item">
                    <TestStoreButton build_store set_test_error_msg />
                </div>
                <div class="level-item">
                    <SaveStoreButton build_store changed />
                </div>
            </div>
        </nav>
        <div
            class="notification is-warning"
            class:is-hidden=move || test_error_msg.get().is_empty()
        >
            <button class="delete" on:click=move |_| set_test_error_msg.set(String::new())></button>
            {move || format!("{}", test_error_msg.get())}
        </div>
        <div class="m-4">
            <div class="mb-2 field is-horizontal">
                <div class="field-label is-normal">
                    <label class="label" for="label-input">
                        Label
                    </label>
                </div>
                <div class="field-body">
                    <div class="field">
                        <p class="control is-expanded has-icons-left">
                            <input
                                class="input"
                                type="text"
                                id="label-input"
                                node_ref=label_input_ref
                                placeholder="Descriptive label for the pack store."
                                value=store.get_value().label
                            />
                            <span class="icon is-small is-left">
                                <i class="fa-solid fa-quote-left"></i>
                            </span>
                        </p>
                    </div>
                </div>
            </div>

            <div class="mb-2 field is-horizontal">
                <div class="field-label is-normal">
                    <label class="label" for="addr-input">
                        Remote Address
                    </label>
                </div>
                <div class="field-body">
                    <div class="field">
                        <p class="control is-expanded has-icons-left">
                            <input
                                class="input"
                                type="text"
                                id="addr-input"
                                node_ref=addr_input_ref
                                placeholder="Host and port of S-FTP server."
                                value=remote_addr
                            />
                            <span class="icon is-small is-left">
                                <i class="fa-solid fa-globe"></i>
                            </span>
                        </p>
                    </div>
                </div>
            </div>

            <div class="mb-2 field is-horizontal">
                <div class="field-label is-normal">
                    <label class="label" for="username-input">
                        Username
                    </label>
                </div>
                <div class="field-body">
                    <div class="field">
                        <p class="control is-expanded has-icons-left">
                            <input
                                class="input"
                                type="text"
                                id="username-input"
                                node_ref=username_input_ref
                                placeholder="Name of user account."
                                value=username
                            />
                            <span class="icon is-small is-left">
                                <i class="fa-solid fa-user"></i>
                            </span>
                        </p>
                    </div>
                </div>
            </div>

            <div class="mb-2 field is-horizontal">
                <div class="field-label is-normal">
                    <label class="label" for="password-input">
                        Password
                    </label>
                </div>
                <div class="field-body">
                    <div class="field has-addons">
                        <div class="control is-expanded has-icons-left">
                            <input
                                class="input"
                                type=move || if secret_visible.get() { "text" } else { "password" }
                                id="password-input"
                                node_ref=passwd_input_ref
                                placeholder="Password for user account."
                                value=password
                            />
                            <span class="icon is-small is-left">
                                <i class="fa-solid fa-key"></i>
                            </span>
                        </div>
                    </div>
                    <div class="control">
                    <button
                        class="button"
                        on:click=move |_| secret_visible.update(|v| { *v = !*v })
                    >
                        <span class="icon is-small">
                            <i class=move || {
                                if secret_visible.get() {
                                    "fas fa-eye-slash"
                                } else {
                                    "fas fa-eye"
                                }
                            }></i>
                        </span>
                    </button>
                </div>
        </div>
            </div>

            <div class="field is-horizontal">
                <div class="field-label is-normal">
                    <label class="label" for="basepath-input">
                        Base Path
                    </label>
                </div>
                <div class="field-body">
                    <div class="field">
                        <p class="control is-expanded has-icons-left">
                            <input
                                class="input"
                                type="text"
                                id="basepath-input"
                                node_ref=path_input_ref
                                placeholder="Path for remote storage."
                                value=basepath
                            />
                            <span class="icon is-small is-left">
                                <i class="fa-solid fa-folder"></i>
                            </span>
                        </p>
                    </div>
                </div>
            </div>
        </div>
    }
}
