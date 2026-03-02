//
// Copyright (c) 2026 Nathan Fiedler
//
import { A } from '@solidjs/router';
import ColorTheme from './color-theme.tsx';

function Navbar() {
  return (
    <nav class="navbar" role="navigation" aria-label="main navigation">
      <div class="navbar-brand">
        <button class="button is-large is-rounded is-static">
          <span class="icon">
            <i class="fa-solid fa-clock-rotate-left"></i>
          </span>
        </button>
        <a
          role="button"
          class="navbar-burger"
          aria-label="menu"
          aria-expanded="false"
          data-target="navbarMenu"
        >
          <span aria-hidden="true"></span>
          <span aria-hidden="true"></span>
          <span aria-hidden="true"></span>
          <span aria-hidden="true"></span>
        </a>
      </div>

      <div id="navbarMenu" class="navbar-menu">
        <div class="navbar-start">
          <A class="navbar-item" href="/" end={true}>
            Home
          </A>

          <A class="navbar-item" href="/snapshots">
            Snapshots
          </A>

          <A class="navbar-item" href="/datasets">
            Datasets
          </A>

          <A class="navbar-item" href="/stores">
            Stores
          </A>

          <A class="navbar-item" href="/restore">
            Restore
          </A>
        </div>

        <div class="navbar-end">
          <div class="navbar-item">
            <ColorTheme />
          </div>
        </div>
      </div>
    </nav>
  );
}

export default Navbar;
