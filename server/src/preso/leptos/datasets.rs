//
// Copyright (c) 2025 Nathan Fiedler
//
use crate::domain::entities::schedule::{Schedule, TimeRange};
use crate::domain::entities::{Dataset, SnapshotRetention, Store};
use crate::preso::leptos::nav;
use leptos::html::Input;
use leptos::prelude::*;
use leptos_router::components::Outlet;
use leptos_router::hooks::use_params_map;
use std::collections::HashSet;
use std::path::Path;

///
/// Retrieve a single dataset by its identifier.
///
#[leptos::server(name = GetDataset, prefix = "/api", input = server_fn::codec::Cbor)]
pub async fn get_dataset(id: Option<String>) -> Result<Option<Dataset>, ServerFnError> {
    use crate::domain::usecases::get_datasets::GetDatasets;
    use crate::domain::usecases::{NoParams, UseCase};
    use leptos::server_fn::error::ServerFnErrorErr;

    // calling get_datasets() to get one dataset is hardly efficient but for now
    // it is not bad enough to add another use case
    if let Some(id) = id {
        let repo = super::ssr::db()?;
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
    }
    Ok(None)
}

///
/// Create a new dataset with the given values, returning its identifier.
///
#[leptos::server(name = CreateDataset, prefix = "/api", input = server_fn::codec::Cbor)]
async fn create_dataset(dataset: Dataset) -> Result<String, ServerFnError> {
    use crate::domain::usecases::new_dataset::{NewDataset, Params};
    use crate::domain::usecases::UseCase;
    use server_fn::error::ServerFnErrorErr;

    let repo = super::ssr::db()?;
    let usecase = NewDataset::new(Box::new(repo));
    let params: Params = Params::from(dataset);
    let result = usecase
        .call(params)
        .map_err(|e| ServerFnErrorErr::ServerError(e.to_string()))?;
    Ok(result.id)
}

///
/// Update the dataset with the given values.
///
#[leptos::server(name = UpdateDataset, prefix = "/api", input = server_fn::codec::Cbor)]
async fn update_dataset(dataset: Dataset) -> Result<(), ServerFnError> {
    use crate::domain::usecases::update_dataset::{Params, UpdateDataset};
    use crate::domain::usecases::UseCase;
    use server_fn::error::ServerFnErrorErr;

    let repo = super::ssr::db()?;
    let usecase = UpdateDataset::new(Box::new(repo));
    let params: Params = Params::from(dataset);
    let result = usecase.call(params);
    if let Err(ref err) = result {
        log::error!("dataset update failed: {}", err);
    }
    result.map_err(|e| ServerFnErrorErr::ServerError(e.to_string()))?;
    Ok(())
}

///
/// Delete the dataset with the given identifier.
///
#[leptos::server]
async fn delete_dataset(id: String) -> Result<(), ServerFnError> {
    use crate::domain::usecases::delete_dataset::{DeleteDataset, Params};
    use crate::domain::usecases::UseCase;
    use server_fn::error::ServerFnErrorErr;

    let repo = super::ssr::db()?;
    let usecase = DeleteDataset::new(Box::new(repo));
    let params: Params = Params::new(id);
    usecase
        .call(params)
        .map_err(|e| ServerFnErrorErr::ServerError(e.to_string()))?;
    Ok(())
}

fn format_schedule(schedule: Option<&Schedule>) -> String {
    if let Some(value) = schedule {
        value.to_string()
    } else {
        String::from("not scheduled")
    }
}

#[component]
pub fn DatasetsPage() -> impl IntoView {
    let location = leptos_router::hooks::use_location();
    let datasets_resource = Resource::new(
        // use location memo as a hack to refetch the datasets whenever a new
        // one is added or removed, which results in the path changing
        move || location.pathname.get(),
        |_| async move {
            // sort the datasets by identifier for consistent ordering
            let mut results = super::datasets().await;
            if let Ok(data) = results.as_mut() {
                data.sort_by(|a, b| a.id.cmp(&b.id));
            }
            results
        },
    );
    let create_dataset = Action::new(move |_input: &()| async move {
        let basepath = Path::new("/");
        let dummy = Dataset::new(basepath);
        match create_dataset(dummy).await {
            Ok(id) => {
                datasets_resource.refetch();
                let navigate = leptos_router::hooks::use_navigate();
                let url = format!("/datasets/{}", id);
                navigate(&url, Default::default());
            }
            Err(err) => {
                log::error!("dataset create failed: {err:#?}");
            }
        }
    });

    view! {
        <nav::NavBar />

        <div class="container">
            <nav class="level">
                <div class="level-left">
                    <div class="level-item">
                        <button
                            class="button"
                            on:click=move |_| {
                                create_dataset.dispatch(());
                            }
                        >
                            <span class="icon">
                                <i class="fa-solid fa-circle-plus"></i>
                            </span>
                            <span>New Dataset</span>
                        </button>
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
                                datasets_resource
                                    .get()
                                    .map(|result| match result {
                                        Err(err) => {
                                            view! { <span>{move || format!("Error: {}", err)}</span> }
                                                .into_any()
                                        }
                                        Ok(datasets) => {
                                            let stored = StoredValue::new(datasets);
                                            view! {
                                                <div class="list has-hoverable-list-items has-overflow-ellipsis">
                                                    <For
                                                        each=move || {
                                                            stored.get_value().into_iter().map(|s| StoredValue::new(s))
                                                        }
                                                        key=|s| s.get_value().id
                                                        let:dataset
                                                    >
                                                        <div
                                                            class="list-item"
                                                            on:click=move |_| {
                                                                let navigate = leptos_router::hooks::use_navigate();
                                                                let url = format!("/datasets/{}", dataset.get_value().id);
                                                                navigate(&url, Default::default());
                                                            }
                                                        >
                                                            <div class="list-item-content">
                                                                <div class="list-item-title">
                                                                    {format!("{}", dataset.get_value().basepath.display())}
                                                                </div>
                                                                <div class="list-item-description">
                                                                    {format_schedule(dataset.get_value().schedules.first())}
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
                            datasets_resource
                                .get()
                                .map(|result| match result {
                                    Err(err) => {
                                        view! { <span>{move || format!("Error: {}", err)}</span> }
                                            .into_any()
                                    }
                                    Ok(datasets) => {
                                        if datasets.is_empty() {
                                            view! {
                                                <div class="container">
                                                    <p class="m-2 title is-5">No datasets defined.</p>
                                                    <p class="subtitle is-5">
                                                        Use the <strong class="mx-1">New Dataset</strong>
                                                        button to create a dataset.
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
pub fn DatasetDetails() -> impl IntoView {
    let params = use_params_map();
    let dataset_resource = Resource::new(
        move || params.with(|params| params.get("id")),
        |id| async move { get_dataset(id).await },
    );
    let values_changed = Action::new(move |_input: &()| {
        async move {
            // for now, just cause the page to reload to reflect whatever changes
            // were made, including deleting or updating a dataset
            let navigate = leptos_router::hooks::use_navigate();
            navigate("/datasets", Default::default());
        }
    });

    view! {
        <Transition fallback=move || {
            view! { "Loading..." }
        }>
            {move || {
                dataset_resource
                    .get()
                    .map(|result| match result {
                        Err(err) => {
                            view! { <span>{move || format!("Error: {}", err)}</span> }.into_any()
                        }
                        Ok(details) => {
                            if let Some(dataset) = details {
                                view! {
                                    <DatasetForm
                                        dataset
                                        changed=move || {
                                            values_changed.dispatch(());
                                        }
                                    />
                                }
                                    .into_any()
                            } else {
                                view! {
                                    <div class="m-4">
                                        <p class="subtitle is-5">
                                            Error: no dataset with that identifier.
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
fn DatasetForm<E>(dataset: Dataset, changed: E) -> impl IntoView
where
    E: Fn() + Copy + 'static + Send,
{
    // all available stores to complete the list of checkboxes
    let stores_resource = Resource::new(
        || (),
        |_| async move {
            // sort the stores by identifier for consistent ordering
            let mut results = super::stores().await;
            if let Ok(data) = results.as_mut() {
                data.sort_by(|a, b| a.id.cmp(&b.id));
            }
            results
        },
    );
    let basepath_input_ref: NodeRef<Input> = NodeRef::new();
    let basepath: String = dataset.basepath.display().to_string();
    let excludes_input_ref: NodeRef<Input> = NodeRef::new();
    let excludes: String = dataset.excludes.join(", ");
    let packsize_input_ref: NodeRef<Input> = NodeRef::new();
    // convert pack size bytes to megabytes
    let packsize = dataset.pack_size / 1048576;

    // scheduling
    let (frequency, set_frequency) = signal(match dataset.schedules.first() {
        None => "manual",
        Some(Schedule::Hourly) => "hourly",
        Some(Schedule::Daily(_)) => "daily",
        Some(Schedule::Weekly(_)) => "manual",
        Some(Schedule::Monthly(_)) => "manual",
    });
    let start_time = RwSignal::new(match dataset.schedules.first() {
        Some(Schedule::Daily(Some(ref range))) => range.format_start(),
        _ => String::from("00:00"),
    });
    let stop_time = RwSignal::new(match dataset.schedules.first() {
        Some(Schedule::Daily(Some(ref range))) => range.format_stop(),
        _ => String::from("00:00"),
    });
    let time_input_disabled = move || frequency.with(|v| *v != "daily");

    // snapshot retention
    let (retention, set_retention) = signal(match dataset.retention {
        SnapshotRetention::ALL => "all",
        SnapshotRetention::COUNT(_) => "count",
        SnapshotRetention::DAYS(_) => "days",
    });
    let retain_count = RwSignal::new(match dataset.retention {
        SnapshotRetention::COUNT(c) => format!("{}", c),
        _ => String::from("10"),
    });
    let retain_count_disabled = move || retention.with(|v| *v != "count");
    let retain_days = RwSignal::new(match dataset.retention {
        SnapshotRetention::DAYS(d) => format!("{}", d),
        _ => String::from("10"),
    });
    let retain_days_disabled = move || retention.with(|v| *v != "days");

    let selected_stores: RwSignal<HashSet<String>> =
        RwSignal::new(dataset.stores.iter().map(|s| s.to_owned()).collect());
    let dataset_id = StoredValue::new(dataset.id.clone());
    let build_dataset = move || {
        let new_basepath = basepath_input_ref.get().unwrap().value();
        let basepath = Path::new(&new_basepath);
        let mut new_dataset = Dataset::new(basepath);
        new_dataset.id = dataset_id.get_value();
        // set the schedule(s)
        if frequency.get() == "hourly" {
            new_dataset.add_schedule(Schedule::Hourly);
        } else if frequency.get() == "daily" {
            let start = start_time.get();
            let stop = stop_time.get();
            let range = TimeRange::parse_from_str(&start, &stop);
            new_dataset.add_schedule(Schedule::Daily(Some(range)));
        }
        for store in selected_stores.read().iter() {
            new_dataset.add_store(store);
        }
        // split excludes by comma, trim, remove empties
        let new_excludes = excludes_input_ref.get().unwrap().value();
        let excludes: Vec<String> = new_excludes
            .split(',')
            .map(|e| e.trim())
            .filter(|e| !e.is_empty())
            .map(|e| e.to_owned())
            .collect();
        new_dataset.excludes = excludes;
        let new_pack_size = packsize_input_ref.get().unwrap().value();
        // convert input megabytes back to pack size bytes
        new_dataset.pack_size = new_pack_size.parse::<u64>().unwrap_or(64) * 1048576;
        // set the retention policy
        if retention.get() == "count" {
            let count_str = retain_count.get();
            let count = count_str.parse::<u16>().unwrap_or(1);
            new_dataset.retention = SnapshotRetention::COUNT(count);
        } else if retention.get() == "days" {
            let days_str = retain_days.get();
            let days = days_str.parse::<u16>().unwrap_or(1);
            new_dataset.retention = SnapshotRetention::DAYS(days);
        }
        new_dataset
    };
    let (is_not_valid, set_is_not_valid) = signal(false);
    let (basepath_error_msg, set_basepath_error_msg) = signal("");
    let (packsize_error_msg, set_packsize_error_msg) = signal("");
    let (stores_error_msg, set_stores_error_msg) = signal("");
    let validate = move || {
        // reset everything first for simpler flow of control
        set_basepath_error_msg.set("");
        set_packsize_error_msg.set("");
        set_stores_error_msg.set("");
        set_is_not_valid.set(false);
        let new_basepath = basepath_input_ref.get().unwrap().value();
        if new_basepath.is_empty() {
            set_basepath_error_msg.set("Base path cannot be empty.");
            set_is_not_valid.set(true);
        }
        let new_pack_size = packsize_input_ref.get().unwrap().value();
        if let Ok(pack_size) = new_pack_size.parse::<u64>() {
            if !(16..=256).contains(&pack_size) {
                set_packsize_error_msg.set("Pack size must be between 16 and 256.");
                set_is_not_valid.set(true);
            }
        } else {
            set_packsize_error_msg.set("Pack size must be a natural number.");
            set_is_not_valid.set(true);
        }
        if selected_stores.read().is_empty() {
            set_stores_error_msg.set("At least one store must be selected.");
            set_is_not_valid.set(true);
        }
    };
    let (save_error_msg, set_save_error_msg) = signal(String::new());

    view! {
        <h2 class="m-4 title">Dataset</h2>
        <nav class="m-4 level">
            <div class="level-left">
                <div class="level-item">
                    <DeleteDatasetButton dataset_id deleted=changed />
                </div>
            </div>
            <div class="level-right">
                <div class="level-item">
                    <SaveDatasetButton is_disabled=is_not_valid build_dataset set_save_error_msg />
                </div>
            </div>
        </nav>
        <div
            class="notification is-warning"
            class:is-hidden=move || save_error_msg.get().is_empty()
        >
            <button class="delete" on:click=move |_| set_save_error_msg.set(String::new())></button>
            {move || save_error_msg.get().to_string()}
        </div>
        <div class="m-4">
            <div class="mb-2 field is-horizontal">
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
                                on:blur=move |_| validate()
                                on:change=move |_| validate()
                            />
                            <span class="icon is-small is-left">
                                <i class="fa-solid fa-folder"></i>
                            </span>
                        </p>
                        <Show when=move || !basepath_error_msg.read().is_empty()>
                            <p class="help is-danger">{format!("{}", basepath_error_msg.read())}</p>
                        </Show>
                    </div>
                </div>
            </div>

            <div class="mb-2 field is-horizontal">
                <div class="field-label is-normal">
                    <label class="label" for="excludes-input">
                        File Exclusions
                    </label>
                </div>
                <div class="field-body">
                    <div class="field">
                        <p class="control is-expanded has-icons-left">
                            <input
                                class="input"
                                type="text"
                                id="excludes-input"
                                node_ref=excludes_input_ref
                                placeholder="Comma-separated file and directory exclusions."
                                value=excludes
                                on:blur=move |_| validate()
                                on:change=move |_| validate()
                            />
                            <span class="icon is-small is-left">
                                <i class="fa-solid fa-file-circle-minus"></i>
                            </span>
                        </p>
                        <p class="help">
                            File patterns to exclude from backup, separated by commas.
                        </p>
                    </div>
                </div>
            </div>

            <div class="mb-2 field is-horizontal">
                <div class="field-label is-normal">
                    <label class="label" for="packsize-input">
                        Pack Size (MB)
                    </label>
                </div>
                <div class="field-body">
                    <div class="field">
                        <p class="control is-expanded has-icons-left">
                            <input
                                class="input"
                                type="number"
                                id="packsize-input"
                                min="16"
                                max="256"
                                step="16"
                                node_ref=packsize_input_ref
                                value=packsize
                                on:blur=move |_| validate()
                                on:change=move |_| validate()
                            />
                            <span class="icon is-small is-left">
                                <i class="fa-solid fa-box"></i>
                            </span>
                        </p>
                        <Show when=move || !packsize_error_msg.read().is_empty()>
                            <p class="help is-danger">{format!("{}", packsize_error_msg.read())}</p>
                        </Show>
                    </div>
                </div>
            </div>

            <div class="mb-2 field is-horizontal">
                <div class="field-label">
                    <label class="label">Pack Stores</label>
                </div>
                <div class="field-body">
                    <div class="field is-narrow">
                        <div class="control">
                            <div class="checkboxes">
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
                                                    view! {
                                                        <StoreCheckboxes stores selected_stores validate />
                                                    }
                                                        .into_any()
                                                }
                                            })
                                    }}
                                </Transition>
                            </div>
                        </div>
                        <Show when=move || !stores_error_msg.read().is_empty()>
                            <p class="help is-danger">{format!("{}", stores_error_msg.read())}</p>
                        </Show>
                    </div>
                </div>
            </div>

            <div class="mb-2 field is-horizontal">
                <div class="field-label">
                    <label class="label">Schedule</label>
                </div>
                <div class="field-body">
                    <div class="field is-narrow">
                        <div class="control">
                            <div class="radios">
                                <label class="radio">
                                    <input
                                        type="radio"
                                        name="frequency"
                                        checked=move || frequency.read() == "manual"
                                        on:change=move |_| {
                                            set_frequency.set("manual");
                                        }
                                    />
                                    Manual
                                </label>
                                <label class="radio">
                                    <input
                                        type="radio"
                                        name="frequency"
                                        checked=move || frequency.read() == "hourly"
                                        on:change=move |_| {
                                            set_frequency.set("hourly");
                                        }
                                    />
                                    Hourly
                                </label>
                                <label class="radio">
                                    <input
                                        type="radio"
                                        name="frequency"
                                        checked=move || frequency.read() == "daily"
                                        on:change=move |_| {
                                            set_frequency.set("daily");
                                        }
                                    />
                                    Daily
                                </label>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            <div class="mb-2 field is-horizontal">
                <div class="field-label is-normal">
                    <label class="label">Start/Stop</label>
                </div>
                <div class="field-body">
                    <div class="field is-grouped">
                        <p class="control has-icons-left">
                            <input
                                class="input"
                                type="time"
                                id="start-time"
                                name="start-time"
                                bind:value=start_time
                                disabled=time_input_disabled
                            />
                            <span class="icon is-left">
                                <i class="fa-solid fa-hourglass-start"></i>
                            </span>
                        </p>
                        <p class="control has-icons-left">
                            <input
                                class="input"
                                type="time"
                                id="stop-time"
                                name="stop-time"
                                bind:value=stop_time
                                disabled=time_input_disabled
                            />
                            <span class="icon is-left">
                                <i class="fa-solid fa-hourglass-end"></i>
                            </span>
                        </p>
                    </div>
                </div>
            </div>

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
                                        checked=move || retention.read() == "all"
                                        on:change=move |_| {
                                            set_retention.set("all");
                                        }
                                    />
                                    All Snapshots
                                </label>
                                <label class="radio">
                                    <input
                                        type="radio"
                                        name="retention"
                                        checked=move || retention.read() == "count"
                                        on:change=move |_| {
                                            set_retention.set("count");
                                        }
                                    />
                                    Limited by Count
                                </label>
                                <label class="radio">
                                    <input
                                        type="radio"
                                        name="retention"
                                        checked=move || retention.read() == "days"
                                        on:change=move |_| {
                                            set_retention.set("days");
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
                    <label class="label" for="retention-count">
                        Count Limit
                    </label>
                </div>
                <div class="field-body">
                    <div class="field">
                        <div class="control">
                            <p class="control">
                                <input
                                    class="input"
                                    type="number"
                                    id="retention-count"
                                    min="1"
                                    max="1024"
                                    bind:value=retain_count
                                    disabled=retain_count_disabled
                                />
                            </p>
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
                                    bind:value=retain_days
                                    disabled=retain_days_disabled
                                />
                            </p>
                        </div>
                    </div>
                </div>
            </div>

        </div>
    }
}

#[component]
fn StoreCheckboxes<F>(
    stores: Vec<Store>,
    selected_stores: RwSignal<HashSet<String>>,
    validate: F,
) -> impl IntoView
where
    F: Fn() + Send + Copy + 'static,
{
    let stored = StoredValue::new(stores);
    view! {
        <For each=move || { stored.get_value() } key=|s| s.id.clone() let:elem>
            {move || {
                let checked = StoredValue::new(selected_stores.with(|l| l.contains(&elem.id)));
                let store = StoredValue::new(elem.clone());
                view! {
                    <label class="checkbox">
                        <input
                            type="checkbox"
                            name="store"
                            checked=move || checked.get_value()
                            on:change=move |_| {
                                let id = store.get_value().id;
                                selected_stores
                                    .update(|list| {
                                        if list.contains(&id) {
                                            list.remove(&id);
                                        } else {
                                            list.insert(id);
                                        }
                                    });
                                validate()
                            }
                        />
                        {format!(
                            "[{}] {}",
                            store.get_value().store_type,
                            store.get_value().label,
                        )}
                    </label>
                }
            }}
        </For>
    }
    .into_any()
}

#[component]
fn DeleteDatasetButton<E>(dataset_id: StoredValue<String>, deleted: E) -> impl IntoView
where
    E: Fn() + Copy + 'static + Send,
{
    let delete_action = Action::new_local(move |_input: &()| {
        let result = delete_dataset(dataset_id.get_value());
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
fn SaveDatasetButton<F>(
    is_disabled: ReadSignal<bool>,
    build_dataset: F,
    set_save_error_msg: WriteSignal<String>,
) -> impl IntoView
where
    F: Fn() -> Dataset + 'static,
{
    let save_action = Action::new_local(move |_input: &()| {
        let dataset = build_dataset();
        update_dataset(dataset)
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
