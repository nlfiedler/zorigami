//
// Copyright (c) 2026 Nathan Fiedler
//
/* @refresh reload */
import { render } from 'solid-js/web';
import { Router, Route } from '@solidjs/router';
import './assets/main.scss';
import { ApolloProvider } from './apollo-provider.tsx';
import Navbar from './components/navbar.tsx';
import Home from './pages/home.tsx';
import {
  SnapshotsPage,
  SnapshotHelp,
  Snapshots,
  SnapshotBrowse
} from './pages/snapshots.tsx';
import { DatasetsPage, Datasets, DatasetDetails } from './pages/datasets.tsx';
import { StoresPage, Stores, StoreDetails } from './pages/stores.tsx';
import Restore from './pages/restore.tsx';
import Settings from './pages/settings.tsx';

function App(props: any) {
  return (
    <>
      <Navbar />
      {props.children}
    </>
  );
}

render(
  () => (
    <ApolloProvider>
      <Router root={App}>
        <Route path="" component={Home} />
        <Route path="/stores" component={StoresPage}>
          <Route path="/" component={Stores} />
          <Route path="/:id" component={StoreDetails} />
        </Route>
        <Route path="/datasets" component={DatasetsPage}>
          <Route path="/" component={Datasets} />
          <Route path="/:id" component={DatasetDetails} />
        </Route>
        <Route path="/snapshots" component={SnapshotsPage}>
          <Route path="/" component={SnapshotHelp} />
          <Route path="/:id" component={Snapshots} />
          <Route path="/:id/browse/:sid" component={SnapshotBrowse} />
        </Route>
        <Route path="/restore" component={Restore} />
        <Route path="/settings" component={Settings} />
        <Route path="*paramName" component={NotFound} />
      </Router>
    </ApolloProvider>
  ),
  document.querySelector('#root')!
);

function NotFound() {
  return (
    <section class="section">
      <h1 class="title">Page not found</h1>
      <h2 class="subtitle">This is not the page you are looking for.</h2>
      <div class="content">
        <p>Try using the navigation options above.</p>
      </div>
    </section>
  );
}
