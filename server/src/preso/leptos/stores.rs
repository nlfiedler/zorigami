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

///
/// Retrieve one pack store.
///
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

///
/// Create a dummy store for the given type.
///
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
            Store {
                id: "auto-generated".into(),
                store_type,
                label: store_type.to_string(),
                properties: props,
                retention: PackRetention::ALL,
            }
        }
        StoreType::AZURE => {
            props.insert("account".into(), "my-storage".into());
            props.insert("access_key".into(), "AKIAIOSFODNN7EXAMPLE".into());
            props.insert("access_tier".into(), "Cool".into());
            props.insert("custom_uri".into(), String::new());
            Store {
                id: "auto-generated".into(),
                store_type,
                label: store_type.to_string(),
                properties: props,
                retention: PackRetention::ALL,
            }
        }
        StoreType::GOOGLE => {
            props.insert(
                "credentials".into(),
                "/Users/charlie/credentials.json".into(),
            );
            props.insert("project".into(), "white-sunspot-12345".into());
            props.insert("region".into(), "us-west1".into());
            props.insert("storage".into(), "NEARLINE".into());
            Store {
                id: "auto-generated".into(),
                store_type,
                label: store_type.to_string(),
                properties: props,
                retention: PackRetention::ALL,
            }
        }
        StoreType::LOCAL => {
            props.insert("basepath".into(), ".".into());
            Store {
                id: "auto-generated".into(),
                store_type,
                label: store_type.to_string(),
                properties: props,
                retention: PackRetention::ALL,
            }
        }
        StoreType::MINIO => {
            props.insert("region".into(), "us-west-1".into());
            props.insert("endpoint".into(), "http://192.168.1.1:9000".into());
            props.insert("access_key".into(), "AKIAIOSFODNN7EXAMPLE".into());
            props.insert(
                "secret_key".into(),
                "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
            );
            Store {
                id: "auto-generated".into(),
                store_type,
                label: store_type.to_string(),
                properties: props,
                retention: PackRetention::ALL,
            }
        }
        StoreType::SFTP => {
            props.insert("remote_addr".into(), "127.0.0.1:22".into());
            props.insert("username".into(), "charlie".into());
            props.insert("password".into(), "secret123".into());
            props.insert("basepath".into(), ".".into());
            Store {
                id: "auto-generated".into(),
                store_type,
                label: store_type.to_string(),
                properties: props,
                retention: PackRetention::ALL,
            }
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
    let result = usecase.call(params);
    if let Err(ref err) = result {
        log::error!("store update failed: {}", err);
    }
    result.map_err(|e| ServerFnErrorErr::ServerError(e.to_string()))?;
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
            let mut results = super::stores().await;
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
                    log::error!("pack store create failed: {err:#?}");
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
            </nav>
            <div class="my-4 columns">
                <div class="column is-one-quarter">
                    <div class="box">
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
                                                <div class="list has-hoverable-list-items has-overflow-ellipsis">
                                                    <For
                                                        each=move || {
                                                            stored.get_value().into_iter().map(|s| StoredValue::new(s))
                                                        }
                                                        key=|s| s.get_value().id
                                                        let:store
                                                    >
                                                        <div
                                                            class="list-item"
                                                            on:click=move |_| {
                                                                let navigate = leptos_router::hooks::use_navigate();
                                                                let url = format!("/stores/{}", store.get_value().id);
                                                                navigate(&url, Default::default());
                                                            }
                                                        >
                                                            <div class="list-item-content">
                                                                <div class="list-item-title">{store.get_value().label}</div>
                                                                <div class="list-item-description">
                                                                    {store.get_value().store_type.to_string()}
                                                                </div>
                                                            </div>
                                                        </div>
                                                    </For>
                                                </div>
                                            }
                                                .into_any()
                                        }
                                    })
                            }}
                        </Transition>
                    </div>
                </div>
                <div class="column">
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
            </div>
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
                                        <p class="subtitle is-5">
                                            Error: no store with that identifier.
                                        </p>
                                        <p>Use the navigation bar to try again.</p>
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
fn DeleteStoreButton<E>(store_id: StoredValue<String>, deleted: E) -> impl IntoView
where
    E: Fn() + Copy + 'static + Send,
{
    let delete_action = Action::new_local(move |_input: &()| {
        let result = delete_store(store_id.get_value());
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
            log::error!("error: {}", err);
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
fn SaveStoreButton<F>(
    is_disabled: Memo<bool>,
    build_store: F,
    set_save_error_msg: WriteSignal<String>,
) -> impl IntoView
where
    F: Fn() -> Store + 'static,
{
    let save_action = Action::new_local(move |_input: &()| {
        let store = build_store();
        update_store(store)
    });
    Effect::new(move |_| {
        // cannot read update_dataset() result inside action and set the signal
        // at the same time (Fn captures environment)
        if let Some(Err(err)) = save_action.value().get() {
            log::error!("error: {}", err);
            set_save_error_msg.set(err.to_string());
        } else {
            set_save_error_msg.set(String::new());
        }
    });
    let save_pending = save_action.pending();
    let save_success = move || {
        save_action
            .value()
            .get()
            .map(|r| r.map(|_| true).unwrap_or(false))
            .unwrap_or(false)
    };

    view! {
        <Show
            when=move || save_pending.get()
            fallback=move || {
                view! {
                    <button
                        class="button is-primary"
                        disabled=move || is_disabled.get()
                        on:click=move |_| {
                            save_action.dispatch(());
                        }
                        aria-disabled="true"
                    >
                        <span class="icon">
                            <i class=move || {
                                if save_success() {
                                    "fas fa-check"
                                } else {
                                    "fa-solid fa-floppy-disk"
                                }
                            }></i>
                        </span>
                        <span>Save</span>
                    </button>
                }
            }
        >
            <button class="button is-loading">Save</button>
        </Show>
    }
}

// Row of buttons for taking action on the store, with status messages to
// provide feedback on the success or failure of the operations.
#[component]
fn StoreActions<E, F>(
    build_store: F,
    store_id: StoredValue<String>,
    is_not_valid: Memo<bool>,
    changed: E,
) -> impl IntoView
where
    E: Fn() + Copy + 'static + Send,
    F: Fn() -> Store + 'static + Copy,
{
    let (test_error_msg, set_test_error_msg) = signal(String::new());
    let (save_error_msg, set_save_error_msg) = signal(String::new());

    view! {
        <nav class="mb-4 level">
            <div class="level-left">
                <div class="level-item">
                    <DeleteStoreButton store_id deleted=changed />
                </div>
            </div>
            <div class="level-right">
                <div class="level-item">
                    <TestStoreButton build_store set_test_error_msg />
                </div>
                <div class="level-item">
                    <SaveStoreButton is_disabled=is_not_valid build_store set_save_error_msg />
                </div>
            </div>
        </nav>
        <div
            class="notification is-warning"
            class:is-hidden=move || test_error_msg.get().is_empty()
        >
            <button class="delete" on:click=move |_| set_test_error_msg.set(String::new())></button>
            {move || test_error_msg.get().to_string()}
        </div>
        <div
            class="notification is-warning"
            class:is-hidden=move || save_error_msg.get().is_empty()
        >
            <button class="delete" on:click=move |_| set_save_error_msg.set(String::new())></button>
            {move || save_error_msg.get().to_string()}
        </div>
    }
}

#[component]
fn StoreLabel(value: RwSignal<String>) -> impl IntoView {
    let error_msg = Memo::new(move |_| {
        value.with(|v| {
            if v.is_empty() {
                "Label must be specified."
            } else {
                ""
            }
        })
    });
    view! {
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
                            placeholder="Descriptive label for the pack store."
                            bind:value=value
                        />
                        <span class="icon is-small is-left">
                            <i class="fa-solid fa-quote-left"></i>
                        </span>
                    </p>
                    <Show when=move || !error_msg.read().is_empty()>
                        <p class="help is-danger">{error_msg.get().to_string()}</p>
                    </Show>
                </div>
            </div>
        </div>
    }
}

#[component]
fn PackRetention(retention: RwSignal<PackRetention>) -> impl IntoView {
    let (retention_kind, set_retention_kind) = signal(retention.with_untracked(|r| match r {
        PackRetention::ALL => "all",
        PackRetention::DAYS(_) => "days",
    }));
    let days_input_ref: NodeRef<Input> = NodeRef::new();
    let retain_days = retention.with_untracked(|r| match r {
        PackRetention::DAYS(d) => format!("{}", d),
        _ => String::from("10"),
    });
    let retain_days_disabled = move || retention_kind.with(|v| *v != "days");
    let update_value = move || {
        if retention_kind.get() == "days" {
            let days_str = days_input_ref.get().unwrap().value();
            let days = days_str.parse::<u16>().unwrap_or(1);
            retention.set(PackRetention::DAYS(days))
        } else {
            retention.set(PackRetention::ALL)
        }
    };

    view! {
        <div class="mb-2 field is-horizontal">
            <div class="field-label">
                <label class="label">Retention</label>
            </div>
            <div class="field-body">
                <div class="field is-narrow">
                    <div class="control">
                        <div class="radios">
                            <label class="radio">
                                <input
                                    type="radio"
                                    name="retention"
                                    checked=move || retention_kind.read() == "all"
                                    on:change=move |_| {
                                        set_retention_kind.set("all");
                                        update_value();
                                    }
                                />
                                All Packs
                            </label>
                            <label class="radio">
                                <input
                                    type="radio"
                                    name="retention"
                                    checked=move || retention_kind.read() == "days"
                                    on:change=move |_| {
                                        set_retention_kind.set("days");
                                        update_value();
                                    }
                                />
                                Limited by Days
                            </label>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <div class="mb-2 field is-horizontal">
            <div class="field-label is-normal">
                <label class="label" for="retention-days">
                    Days Limit
                </label>
            </div>
            <div class="field-body">
                <div class="field">
                    <div class="control">
                        <p class="control">
                            <input
                                class="input"
                                type="number"
                                id="retention-days"
                                min="1"
                                max="1024"
                                node_ref=days_input_ref
                                value=retain_days
                                on:change=move |_| {
                                    update_value();
                                }
                                disabled=retain_days_disabled
                            />
                        </p>
                    </div>
                </div>
            </div>
        </div>
    }
}

#[component]
fn OptionalTextInput(
    label: &'static str,
    name: &'static str,
    value: RwSignal<String>,
    placeholder: &'static str,
    icon: &'static str,
) -> impl IntoView {
    view! {
        <div class="mb-2 field is-horizontal">
            <div class="field-label is-normal">
                <label class="label" for=name>
                    {label}
                </label>
            </div>
            <div class="field-body">
                <div class="field">
                    <p class="control is-expanded has-icons-left">
                        <input
                            class="input"
                            type="text"
                            id=name
                            placeholder=placeholder
                            bind:value=value
                        />
                        <span class="icon is-small is-left">
                            <i class=icon></i>
                        </span>
                    </p>
                </div>
            </div>
        </div>
    }
}

#[component]
fn RequiredTextInput(
    label: &'static str,
    name: &'static str,
    value: RwSignal<String>,
    placeholder: &'static str,
    icon: &'static str,
) -> impl IntoView {
    let error_msg = Memo::new(move |_| {
        value.with(|v| {
            if v.is_empty() {
                format!("A value for {} is required.", label)
            } else {
                String::new()
            }
        })
    });
    view! {
        <div class="mb-2 field is-horizontal">
            <div class="field-label is-normal">
                <label class="label" for=name>
                    {label}
                </label>
            </div>
            <div class="field-body">
                <div class="field">
                    <p class="control is-expanded has-icons-left">
                        <input
                            class="input"
                            type="text"
                            id=name
                            placeholder=placeholder
                            bind:value=value
                        />
                        <span class="icon is-small is-left">
                            <i class=icon></i>
                        </span>
                    </p>
                    <Show when=move || !error_msg.read().is_empty()>
                        <p class="help is-danger">{error_msg.get().to_string()}</p>
                    </Show>
                </div>
            </div>
        </div>
    }
}

#[component]
fn RequiredHiddenInput(
    label: &'static str,
    name: &'static str,
    value: RwSignal<String>,
    placeholder: &'static str,
    icon: &'static str,
) -> impl IntoView {
    let error_msg = Memo::new(move |_| {
        value.with(|v| {
            if v.is_empty() {
                format!("A value for {} is required.", label)
            } else {
                String::new()
            }
        })
    });
    let secret_visible = RwSignal::new(false);

    view! {
        <div class="mb-2 field is-horizontal">
            <div class="field-label is-normal">
                <label class="label" for=name>
                    {label}
                </label>
            </div>
            <div class="field-body">
                <div class="field is-expanded">
                    <div class="field has-addons">
                        <p class="control is-expanded has-icons-left">
                            <input
                                class="input"
                                type=move || if secret_visible.get() { "text" } else { "password" }
                                id=name
                                placeholder=placeholder
                                bind:value=value
                            />
                            <span class="icon is-small is-left">
                                <i class=icon></i>
                            </span>
                        </p>
                        <p class="control">
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
                        </p>
                    </div>
                    <Show when=move || !error_msg.read().is_empty()>
                        <p class="help is-danger">{error_msg.get().to_string()}</p>
                    </Show>
                </div>
            </div>
        </div>
    }
}

#[component]
fn LocalStoreForm<E>(store: Store, changed: E) -> impl IntoView
where
    E: Fn() + Copy + 'static + Send,
{
    let label_value = RwSignal::new(store.label.clone());
    let basepath_value = RwSignal::new(match store.properties.get("basepath") {
        Some(value) => value.to_owned(),
        None => String::new(),
    });
    let store_id = StoredValue::new(store.id.clone());
    let retention = RwSignal::new(store.retention);
    let build_store = move || {
        let new_label = label_value.get();
        let new_basepath = basepath_value.get();
        let mut props: HashMap<String, String> = HashMap::new();
        props.insert("basepath".into(), new_basepath);
        Store {
            id: store_id.get_value(),
            store_type: StoreType::LOCAL,
            label: new_label,
            properties: props,
            retention: retention.get_untracked(),
        }
    };
    let is_not_valid = Memo::new(move |_| {
        let label_invalid = label_value.with(|v| v.is_empty());
        let path_invalid = basepath_value.with(|v| v.is_empty());
        label_invalid || path_invalid
    });

    view! {
        <h2 class="m-4 title">Attached disk</h2>
        <div class="m-4">
            <StoreActions build_store store_id is_not_valid changed />
        </div>
        <div class="m-4">
            <StoreLabel value=label_value />
            <RequiredTextInput
                label="Base Path"
                name="basepath-input"
                value=basepath_value
                placeholder="Path to the local storage."
                icon="fa-solid fa-folder"
            />
            <PackRetention retention />
        </div>
    }
}

#[component]
fn AmazonStoreForm<E>(store: Store, changed: E) -> impl IntoView
where
    E: Fn() + Copy + 'static + Send,
{
    let label_value = RwSignal::new(store.label.clone());
    let class_input_ref: NodeRef<Select> = NodeRef::new();
    let region_value = RwSignal::new(match store.properties.get("region") {
        Some(value) => value.to_owned(),
        None => String::new(),
    });
    let access_key_value = RwSignal::new(match store.properties.get("access_key") {
        Some(value) => value.to_owned(),
        None => String::new(),
    });
    let secret_key_value = RwSignal::new(match store.properties.get("secret_key") {
        Some(value) => value.to_owned(),
        None => String::new(),
    });
    let storage = if let Some(value) = store.properties.get("storage") {
        StoredValue::new(value.to_owned())
    } else {
        StoredValue::new(String::new())
    };
    let store_id = StoredValue::new(store.id.clone());
    let retention = RwSignal::new(store.retention);
    let build_store = move || {
        let new_label = label_value.get();
        let new_region = region_value.get();
        let new_access = access_key_value.get();
        let new_secret = secret_key_value.get();
        let new_class = class_input_ref.get().unwrap().value();
        let mut props: HashMap<String, String> = HashMap::new();
        props.insert("region".into(), new_region);
        props.insert("access_key".into(), new_access);
        props.insert("secret_key".into(), new_secret);
        props.insert("storage".into(), new_class);
        Store {
            id: store_id.get_value(),
            store_type: StoreType::AMAZON,
            label: new_label,
            properties: props,
            retention: retention.get_untracked(),
        }
    };
    let is_not_valid = Memo::new(move |_| {
        label_value.with(|v| v.is_empty())
            || region_value.with(|v| v.is_empty())
            || access_key_value.with(|v| v.is_empty())
            || secret_key_value.with(|v| v.is_empty())
    });

    view! {
        <h2 class="m-4 title">Amazon S3</h2>
        <div class="m-4">
            <StoreActions build_store store_id is_not_valid changed />
        </div>
        <div class="m-4">
            <StoreLabel value=label_value />
            <RequiredTextInput
                label="Region"
                name="region-input"
                value=region_value
                placeholder="Geographic region or availability zone."
                icon="fa-solid fa-globe"
            />
            <RequiredTextInput
                label="Access Key"
                name="access-input"
                value=access_key_value
                placeholder="Access key identifier."
                icon="fa-solid fa-circle-info"
            />
            <RequiredHiddenInput
                label="Secret Key"
                name="secret-input"
                value=secret_key_value
                placeholder="Secret access key."
                icon="fa-solid fa-key"
            />

            <div class="mb-2 field is-horizontal">
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

            <PackRetention retention />
        </div>
    }
}

#[component]
fn AzureStoreForm<E>(store: Store, changed: E) -> impl IntoView
where
    E: Fn() + Copy + 'static + Send,
{
    let label_value = RwSignal::new(store.label.clone());
    let tier_input_ref: NodeRef<Select> = NodeRef::new();
    let account_value = RwSignal::new(match store.properties.get("account") {
        Some(value) => value.to_owned(),
        None => String::new(),
    });
    let access_key_value = RwSignal::new(match store.properties.get("access_key") {
        Some(value) => value.to_owned(),
        None => String::new(),
    });
    let access_tier = if let Some(value) = store.properties.get("access_tier") {
        StoredValue::new(value.to_owned())
    } else {
        StoredValue::new(String::new())
    };
    let custom_uri_value = RwSignal::new(match store.properties.get("custom_uri") {
        Some(value) => value.to_owned(),
        None => String::new(),
    });
    let store_id = StoredValue::new(store.id.clone());
    let retention = RwSignal::new(store.retention);
    let build_store = move || {
        let new_label = label_value.get();
        let new_account = account_value.get();
        let new_access = access_key_value.get();
        let new_tier = tier_input_ref.get().unwrap().value();
        let new_uri = custom_uri_value.get();
        let mut props: HashMap<String, String> = HashMap::new();
        props.insert("account".into(), new_account);
        props.insert("access_key".into(), new_access);
        props.insert("access_tier".into(), new_tier);
        if !new_uri.is_empty() {
            // only set the custom_uri if the value is non-empty
            props.insert("custom_uri".into(), new_uri);
        }
        Store {
            id: store_id.get_value(),
            store_type: StoreType::AZURE,
            label: new_label,
            properties: props,
            retention: retention.get_untracked(),
        }
    };
    let is_not_valid = Memo::new(move |_| {
        label_value.with(|v| v.is_empty())
            || account_value.with(|v| v.is_empty())
            || access_key_value.with(|v| v.is_empty())
    });

    view! {
        <h2 class="m-4 title">Azure Blob Storage</h2>
        <div class="m-4">
            <StoreActions build_store store_id is_not_valid changed />
        </div>
        <div class="m-4">
            <StoreLabel value=label_value />
            <RequiredTextInput
                label="Account Name"
                name="account-input"
                value=account_value
                placeholder="Name of the storage account."
                icon="fa-solid fa-cloud"
            />
            <RequiredHiddenInput
                label="Access Key"
                name="access-input"
                value=access_key_value
                placeholder="Access key."
                icon="fa-solid fa-key"
            />

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

            <OptionalTextInput
                label="Custom URI"
                name="uri-input"
                value=custom_uri_value
                placeholder="Custom URI."
                icon="fa-solid fa-link"
            />
            <PackRetention retention />
        </div>
    }
}

#[component]
fn GoogleStoreForm<E>(store: Store, changed: E) -> impl IntoView
where
    E: Fn() + Copy + 'static + Send,
{
    let label_value = RwSignal::new(store.label.clone());
    let class_input_ref: NodeRef<Select> = NodeRef::new();
    let credentials_value = RwSignal::new(match store.properties.get("credentials") {
        Some(value) => value.to_owned(),
        None => String::new(),
    });
    let project_id_value = RwSignal::new(match store.properties.get("project") {
        Some(value) => value.to_owned(),
        None => String::new(),
    });
    let region_value = RwSignal::new(match store.properties.get("region") {
        Some(value) => value.to_owned(),
        None => String::new(),
    });
    let storage = if let Some(value) = store.properties.get("storage") {
        StoredValue::new(value.to_owned())
    } else {
        StoredValue::new(String::new())
    };
    let store_id = StoredValue::new(store.id.clone());
    let retention = RwSignal::new(store.retention);
    let build_store = move || {
        let new_label = label_value.get();
        let new_creds = credentials_value.get();
        let new_project = project_id_value.get();
        let new_region = region_value.get();
        let new_class = class_input_ref.get().unwrap().value();
        let mut props: HashMap<String, String> = HashMap::new();
        props.insert("credentials".into(), new_creds);
        props.insert("project".into(), new_project);
        props.insert("region".into(), new_region);
        props.insert("storage".into(), new_class);
        Store {
            id: store_id.get_value(),
            store_type: StoreType::GOOGLE,
            label: new_label,
            properties: props,
            retention: retention.get_untracked(),
        }
    };
    let is_not_valid = Memo::new(move |_| {
        label_value.with(|v| v.is_empty())
            || credentials_value.with(|v| v.is_empty())
            || project_id_value.with(|v| v.is_empty())
            || region_value.with(|v| v.is_empty())
    });

    view! {
        <h2 class="m-4 title">Google Cloud Storage</h2>
        <div class="m-4">
            <StoreActions build_store store_id is_not_valid changed />
        </div>
        <div class="m-4">
            <StoreLabel value=label_value />
            <RequiredTextInput
                label="Credentials File"
                name="credentials-input"
                value=credentials_value
                placeholder="Path to JSON credentials file."
                icon="fa-solid fa-key"
            />
            <RequiredTextInput
                label="Project ID"
                name="project-id-input"
                value=project_id_value
                placeholder="Project identifier."
                icon="fa-solid fa-cloud"
            />
            <RequiredTextInput
                label="Region"
                name="region-input"
                value=region_value
                placeholder="Geographic region or availability zone."
                icon="fa-solid fa-globe"
            />

            <div class="mb-2 field is-horizontal">
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

            <PackRetention retention />
        </div>
    }
}

#[component]
fn MinioStoreForm<E>(store: Store, changed: E) -> impl IntoView
where
    E: Fn() + Copy + 'static + Send,
{
    let label_value = RwSignal::new(store.label.clone());
    let region_value = RwSignal::new(match store.properties.get("region") {
        Some(value) => value.to_owned(),
        None => String::new(),
    });
    let endpoint_value = RwSignal::new(match store.properties.get("endpoint") {
        Some(value) => value.to_owned(),
        None => String::new(),
    });
    let access_key_value = RwSignal::new(match store.properties.get("access_key") {
        Some(value) => value.to_owned(),
        None => String::new(),
    });
    let secret_key_value = RwSignal::new(match store.properties.get("secret_key") {
        Some(value) => value.to_owned(),
        None => String::new(),
    });
    let store_id = StoredValue::new(store.id.clone());
    let retention = RwSignal::new(store.retention);
    let build_store = move || {
        let new_label = label_value.get();
        let new_region = region_value.get();
        let new_endpoint = endpoint_value.get();
        let new_access = access_key_value.get();
        let new_secret = secret_key_value.get();
        let mut props: HashMap<String, String> = HashMap::new();
        props.insert("region".into(), new_region);
        props.insert("endpoint".into(), new_endpoint);
        props.insert("access_key".into(), new_access);
        props.insert("secret_key".into(), new_secret);
        Store {
            id: store_id.get_value(),
            store_type: StoreType::MINIO,
            label: new_label,
            properties: props,
            retention: retention.get_untracked(),
        }
    };
    let is_not_valid = Memo::new(move |_| {
        label_value.with(|v| v.is_empty())
            || region_value.with(|v| v.is_empty())
            || endpoint_value.with(|v| v.is_empty())
            || access_key_value.with(|v| v.is_empty())
            || secret_key_value.with(|v| v.is_empty())
    });

    view! {
        <h2 class="m-4 title">MinIO Object Storage</h2>
        <div class="m-4">
            <StoreActions build_store store_id is_not_valid changed />
        </div>
        <div class="m-4">
            <StoreLabel value=label_value />
            <RequiredTextInput
                label="Region"
                name="region-input"
                value=region_value
                placeholder="Geographic region or availability zone."
                icon="fa-solid fa-globe"
            />
            <RequiredTextInput
                label="Endpoint"
                name="endpoint-input"
                value=endpoint_value
                placeholder="Endpoint URL."
                icon="fa-solid fa-link"
            />
            <RequiredTextInput
                label="Access Key"
                name="access-input"
                value=access_key_value
                placeholder="Access key identifier."
                icon="fa-solid fa-circle-info"
            />
            <RequiredHiddenInput
                label="Secret Key"
                name="secret-input"
                value=secret_key_value
                placeholder="Secret access key."
                icon="fa-solid fa-key"
            />
            <PackRetention retention />
        </div>
    }
}

#[component]
fn SftpStoreForm<E>(store: Store, changed: E) -> impl IntoView
where
    E: Fn() + Copy + 'static + Send,
{
    let label_value = RwSignal::new(store.label.clone());
    let address_value = RwSignal::new(match store.properties.get("remote_addr") {
        Some(value) => value.to_owned(),
        None => String::new(),
    });
    let username_value = RwSignal::new(match store.properties.get("username") {
        Some(value) => value.to_owned(),
        None => String::new(),
    });
    let password_value = RwSignal::new(match store.properties.get("password") {
        Some(value) => value.to_owned(),
        None => String::new(),
    });
    let basepath_value = RwSignal::new(match store.properties.get("basepath") {
        Some(value) => value.to_owned(),
        None => String::new(),
    });
    let store_id = StoredValue::new(store.id.clone());
    let retention = RwSignal::new(store.retention);
    let build_store = move || {
        let new_label = label_value.get();
        let new_addr = address_value.get();
        let new_username = username_value.get();
        let new_password = password_value.get();
        let new_path = basepath_value.get();
        let mut props: HashMap<String, String> = HashMap::new();
        props.insert("remote_addr".into(), new_addr);
        props.insert("username".into(), new_username);
        props.insert("password".into(), new_password);
        props.insert("basepath".into(), new_path);
        Store {
            id: store_id.get_value(),
            store_type: StoreType::SFTP,
            label: new_label,
            properties: props,
            retention: retention.get_untracked(),
        }
    };
    let is_not_valid = Memo::new(move |_| {
        label_value.with(|v| v.is_empty())
            || address_value.with(|v| v.is_empty())
            || username_value.with(|v| v.is_empty())
            || password_value.with(|v| v.is_empty())
            || basepath_value.with(|v| v.is_empty())
    });

    view! {
        <h2 class="m-4 title">Secure FTP</h2>
        <div class="m-4">
            <StoreActions build_store store_id is_not_valid changed />
        </div>
        <div class="m-4">
            <StoreLabel value=label_value />
            <RequiredTextInput
                label="Remote Address"
                name="address-input"
                value=address_value
                placeholder="Host and port of S-FTP server."
                icon="fa-solid fa-cloud"
            />
            <RequiredTextInput
                label="Username"
                name="username-input"
                value=username_value
                placeholder="Name of user account."
                icon="fa-solid fa-user"
            />
            <RequiredHiddenInput
                label="Password"
                name="password-input"
                value=password_value
                placeholder="Password for user account."
                icon="fa-solid fa-key"
            />
            <RequiredTextInput
                label="Base Path"
                name="basepath-input"
                value=basepath_value
                placeholder="Path for remote storage."
                icon="fa-solid fa-folder"
            />
            <PackRetention retention />
        </div>
    }
}
