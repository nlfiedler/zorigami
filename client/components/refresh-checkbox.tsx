//
// Copyright (c) 2026 Nathan Fiedler
//
import { createSignal, createEffect, onCleanup } from 'solid-js';

interface AutoRefreshCheckboxProps {
  refetch: () => void;
}

export function AutoRefreshCheckbox(props: AutoRefreshCheckboxProps) {
  const [enabled, setEnabled] = createSignal(false);
  createEffect(() => {
    if (enabled()) {
      const intervalId = setInterval(() => {
        props.refetch();
      }, 5000);
      onCleanup(() => clearInterval(intervalId));
    }
  });

  return (
    <button class="button" on:click={(_) => setEnabled((v) => !v)}>
      <span class="icon">
        <i
          class="fa-regular"
          classList={{
            'fa-square': !enabled(),
            'fa-square-check': enabled()
          }}
        ></i>
      </span>
      <span>Auto-Refresh</span>
    </button>
  );
}
