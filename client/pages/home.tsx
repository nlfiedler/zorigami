//
// Copyright (c) 2026 Nathan Fiedler
//
import { createResource, Suspense } from 'solid-js';
import { type TypedDocumentNode, gql } from '@apollo/client';
import { useApolloClient } from '../apollo-provider';
import { type Query } from 'zorigami/generated/graphql.ts';

const CONFIGURATION: TypedDocumentNode<Query, Record<string, never>> = gql`
  query {
    configuration {
      hostname
      username
      computerId
      computerBucket
    }
  }
`;

function Home() {
  const client = useApolloClient();
  const [confQuery] = createResource(async () => {
    const { data } = await client.query({ query: CONFIGURATION });
    return data;
  });
  return (
    <Suspense fallback={'...'}>
      <ul>
        <li>{confQuery()?.configuration.hostname}</li>
        <li>{confQuery()?.configuration.username}</li>
        <li>{confQuery()?.configuration.computerId}</li>
        <li>{confQuery()?.configuration.computerBucket}</li>
      </ul>
    </Suspense>
  );
}

export default Home;

// use crate::domain::entities::schedule::Schedule;
// use crate::domain::entities::{Checksum, Dataset, Snapshot, SnapshotRetention};
// use crate::preso::leptos::nav;
// use chrono::{DateTime, Utc};
// use leptos::prelude::*;

// #[component]
// pub fn HomePage() -> impl IntoView {
//     let datasets_resource = Resource::new(
//         || {},
//         |_| async move {
//             // sort the datasets by identifier for consistent ordering
//             let mut results = super::datasets().await;
//             if let Ok(data) = results.as_mut() {
//                 data.sort_by(|a, b| a.id.cmp(&b.id));
//             }
//             results
//         },
//     );

//     view! {
//         <nav::NavBar />

//         <Transition fallback=move || {
//             view! { "Loading..." }
//         }>
//             {move || {
//                 datasets_resource
//                     .get()
//                     .map(|result| match result {
//                         Err(err) => {
//                             view! { <span>{move || format!("Error: {}", err)}</span> }.into_any()
//                         }
//                         Ok(datasets) => {
//                             if datasets.is_empty() {
//                                 view! { <NoDatasetsHelp /> }.into_any()
//                             } else {
//                                 let stored = StoredValue::new(datasets);
//                                 view! {
//                                     <div class="container">
//                                         <div class="grid is-col-min-20">
//                                             <For
//                                                 each=move || {
//                                                     stored.get_value().into_iter().map(|s| StoredValue::new(s))
//                                                 }
//                                                 key=|s| s.get_value().id
//                                                 let:dataset
//                                             >
//                                                 <div class="cell">
//                                                     <DatasetCard dataset />
//                                                 </div>
//                                             </For>
//                                         </div>
//                                     </div>
//                                 }
//                                     .into_any()
//                             }
//                         }
//                     })
//             }}
//         </Transition>
//     }
// }

// #[component]
// fn DatasetCard(dataset: StoredValue<Dataset>) -> impl IntoView {
//     let fmt_retention = |rt: SnapshotRetention| -> String {
//         match rt {
//             SnapshotRetention::ALL => String::from("All Snapshots"),
//             SnapshotRetention::COUNT(c) => format!("Retain {} snapshots", c),
//             SnapshotRetention::DAYS(d) => format!("Retain {} days", d),
//         }
//     };
//     let fmt_schedule = |schedule: Option<&Schedule>| -> String {
//         if let Some(value) = schedule {
//             value.to_string()
//         } else {
//             String::from("not scheduled")
//         }
//     };
//     let latest_snapshot = StoredValue::new(dataset.get_value().snapshot);

//     view! {
//         <div class="card">
//             <header class="card-header">
//                 <p class="card-header-title">Dataset</p>
//                 <a href=move || format!("/datasets/{}", dataset.get_value().id)>
//                     <button class="card-header-icon">
//                         <span class="icon">
//                             <i class="fas fa-angle-right"></i>
//                         </span>
//                     </button>
//                 </a>
//             </header>
//             <div class="card-content">
//                 <table class="table is-striped is-fullwidth" style="text-align: start">
//                     <tbody>
//                         <tr>
//                             <td>Base Path</td>
//                             <td>{dataset.get_value().basepath.display().to_string()}</td>
//                         </tr>
//                         <tr>
//                             <td>Schedule</td>
//                             <td>{fmt_schedule(dataset.get_value().schedules.first())}</td>
//                         </tr>
//                         <DatasetStatus dataset />
//                         <SnapshotSummary digest=latest_snapshot />
//                         <SnapshotCount dataset />
//                         <tr>
//                             <td>Retention Policy</td>
//                             <td>{fmt_retention(dataset.get_value().retention)}</td>
//                         </tr>
//                     </tbody>
//                 </table>
//             </div>
//             <footer class="card-footer">
//                 <StartBackupButton />
//                 <a href="#" class="card-footer-item">
//                     <span class="icon">
//                         <i class="fa-solid fa-scissors" aria-hidden="true"></i>
//                     </span>
//                     <span>Prune Snapshots</span>
//                 </a>
//             </footer>
//         </div>
//     }
// }

// #[component]
// fn DatasetStatus(dataset: StoredValue<Dataset>) -> impl IntoView {
//     let status_resource = Resource::new(
//         move || dataset.get_value(),
//         |ds| async move { super::dataset_status(ds.id).await },
//     );
//     view! {
//         <Transition fallback=move || {
//             view! {
//                 <tr>
//                     <td>Status</td>
//                     <td>"Loading..."</td>
//                 </tr>
//             }
//         }>
//             {move || {
//                 status_resource
//                     .get()
//                     .map(|result| match result {
//                         Err(err) => {
//                             view! {
//                                 <tr>
//                                     <td>Status</td>
//                                     <td>{move || format!("Error: {}", err)}</td>
//                                 </tr>
//                             }
//                                 .into_any()
//                         }
//                         Ok(status) => {
//                             view! {
//                                 <tr>
//                                     <td>Status</td>
//                                     <td>{status}</td>
//                                 </tr>
//                             }
//                                 .into_any()
//                         }
//                     })
//             }}
//         </Transition>
//     }
// }

// #[component]
// fn SnapshotSummary(digest: StoredValue<Option<Checksum>>) -> impl IntoView {
//     let snapshot_resource = Resource::new(
//         move || digest.get_value(),
//         |d| async move { super::get_snapshot(d).await },
//     );
//     let count_files = move |snapshot: StoredValue<Snapshot>| -> String {
//         format!("{} files", snapshot.get_value().file_counts.total_files())
//     };

//     view! {
//         <Transition fallback=move || {
//             view! {
//                 <tr>
//                     <td>File Count</td>
//                     <td>"Loading..."</td>
//                 </tr>
//             }
//         }>
//             {move || {
//                 snapshot_resource
//                     .get()
//                     .map(|result| match result {
//                         Err(err) => {
//                             view! {
//                                 <tr>
//                                     <td>File Count</td>
//                                     <td>{move || format!("Error: {}", err)}</td>
//                                 </tr>
//                             }
//                                 .into_any()
//                         }
//                         Ok(maybe_snapshot) => {
//                             match maybe_snapshot {
//                                 Some(snapshot) => {
//                                     let snapshot = StoredValue::new(snapshot);
//                                     view! {
//                                         <tr>
//                                             <td>File Count</td>
//                                             <td>{count_files(snapshot)}</td>
//                                         </tr>
//                                     }
//                                         .into_any()
//                                 }
//                                 None => {
//                                     view! {
//                                         <tr>
//                                             <td>File Count</td>
//                                             <td>0</td>
//                                         </tr>
//                                     }
//                                         .into_any()
//                                 }
//                             }
//                         }
//                     })
//             }}
//         </Transition>
//     }
// }

// #[component]
// fn SnapshotCount(dataset: StoredValue<Dataset>) -> impl IntoView {
//     let count_resource = Resource::new(
//         move || dataset.get_value(),
//         |ds| async move { super::count_snapshots(ds.id).await },
//     );
//     let fmt_datetime = |maybe_dt: Option<DateTime<Utc>>| {
//         if let Some(dt) = maybe_dt {
//             let local = super::convert_utc_to_local(dt);
//             local.format("%Y-%m-%d %H:%M").to_string()
//         } else {
//             String::from("none")
//         }
//     };

//     view! {
//         <Transition fallback=move || {
//             view! {
//                 <tr>
//                     <td>Status</td>
//                     <td>"Loading..."</td>
//                 </tr>
//             }
//         }>
//             {move || {
//                 count_resource
//                     .get()
//                     .map(|result| match result {
//                         Err(err) => {
//                             view! {
//                                 <tr>
//                                     <td>Status</td>
//                                     <td>{move || format!("Error: {}", err)}</td>
//                                 </tr>
//                             }
//                                 .into_any()
//                         }
//                         Ok(counts) => {
//                             view! {
//                                 <tr>
//                                     <td>Snapshot Count</td>
//                                     <td>{counts.count}</td>
//                                 </tr>
//                                 <tr>
//                                     <td>Latest Snapshot</td>
//                                     <td>{fmt_datetime(counts.newest)}</td>
//                                 </tr>
//                                 <tr>
//                                     <td>Oldest Snapshot</td>
//                                     <td>{fmt_datetime(counts.oldest)}</td>
//                                 </tr>
//                             }
//                                 .into_any()
//                         }
//                     })
//             }}
//         </Transition>
//     }
// }

// #[component]
// fn NoDatasetsHelp() -> impl IntoView {
//     let stores_resource = Resource::new(|| (), |_| async move { super::stores().await });

//     view! {
//         <Transition fallback=move || {
//             view! { "Loading..." }
//         }>
//             {move || {
//                 stores_resource
//                     .get()
//                     .map(|result| match result {
//                         Err(err) => {
//                             view! { <span>{move || format!("Error: {}", err)}</span> }.into_any()
//                         }
//                         Ok(stores) => {
//                             if stores.is_empty() {
//                                 view! {
//                                     <p>
//                                         Before defining a new dataset,
//                                         visit the <a href="/stores">Pack Stores</a>
//                                         page to configure a pack store, then visit
//                                         the <a href="/datasets">Datasets</a>
//                                         page to configure a dataset to be backed up.
//                                     </p>
//                                     <p>
//                                         If you wish to restore from a previous backup,
//                                         first visit the <a href="/stores">Pack Stores</a>
//                                         page to configure a pack store, then visit
//                                         the <a href="/restore">Restore</a>page to
//                                         restore from the pack store.
//                                     </p>
//                                 }
//                                     .into_any()
//                             } else {
//                                 view! {
//                                     <p>
//                                         Visit the <a href="/datasets">Datasets</a>
//                                         page to configure a dataset to be backed up.
//                                     </p>
//                                 }
//                                     .into_any()
//                             }
//                         }
//                     })
//             }}
//         </Transition>
//     }
// }

// #[component]
// fn StartBackupButton() -> impl IntoView {
//     // TODO: consider how start/stop button could use an `Action` to wait on a running backup
//     //       * if no backup running, show ~Start~
//     //       * if backup running, wait for finish, action is ~pending~, button shows ~Stop~
//     //       * when action is completed, button shows ~Start~ again
//     // TODO: possibly use a timer to refresh the button based on backup status
//     view! {
//         <a href="#" class="card-footer-item">
//             <span class="icon">
//                 <i class="fa-solid fa-play" aria-hidden="true"></i>
//             </span>
//             <span>Start Backup</span>
//         </a>
//     }
// }
