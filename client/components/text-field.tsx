//
// Copyright (c) 2026 Nathan Fiedler
//
import {
  createMemo,
  createRenderEffect,
  createSignal,
  type Accessor,
  type Setter,
  Show,
  type Signal
} from 'solid-js';

// define a directive to make text input handling more concise
export function textField(
  element: HTMLInputElement,
  value: Accessor<Signal<string>>
) {
  const [field, setField] = value();
  createRenderEffect(() => (element.value = field()));
  element.addEventListener('input', ({ target }) =>
    setField((target as HTMLInputElement).value)
  );
}

// Patch in the field types for the custom directives to satisfy TypeScript.
declare module 'solid-js' {
  namespace JSX {
    interface DirectiveFunctions {
      textField: typeof textField;
    }
  }
}

interface OptionalTextInputProps {
  label: string;
  name: string;
  placeholder: string;
  icon: string;
  field: Accessor<string>;
  setField: Setter<string>;
}

export function OptionalTextInput(props: OptionalTextInputProps) {
  return (
    <div class="mb-2 field is-horizontal">
      <div class="field-label is-normal">
        <label class="label" for={props.name}>
          {props.label}
        </label>
      </div>
      <div class="field-body">
        <div class="field">
          <p class="control is-expanded has-icons-left">
            <input
              class="input"
              type="text"
              id={props.name}
              placeholder={props.placeholder}
              use:textField={[props.field, props.setField]}
            />
            <span class="icon is-small is-left">
              <i class={props.icon}></i>
            </span>
          </p>
        </div>
      </div>
    </div>
  );
}

interface RequiredTextInputProps {
  label: string;
  name: string;
  placeholder: string;
  icon: string;
  field: Accessor<string>;
  setField: Setter<string>;
}

export function RequiredTextInput(props: RequiredTextInputProps) {
  const errorMessage = createMemo(() => {
    const v = props.field();
    if (v.length === 0) {
      return `A value for ${props.label} is required.`;
    } else {
      return '';
    }
  });

  return (
    <div class="mb-2 field is-horizontal">
      <div class="field-label is-normal">
        <label class="label" for={props.name}>
          {props.label}
        </label>
      </div>
      <div class="field-body">
        <div class="field">
          <p class="control is-expanded has-icons-left">
            <input
              class="input"
              type="text"
              id={props.name}
              placeholder={props.placeholder}
              use:textField={[props.field, props.setField]}
              autocomplete="on"
            />
            <span class="icon is-small is-left">
              <i class={props.icon}></i>
            </span>
          </p>
          <Show when={errorMessage().length > 0}>
            <p class="help is-danger">{errorMessage()}</p>
          </Show>
        </div>
      </div>
    </div>
  );
}

interface RequiredHiddenInputProps {
  label: string;
  name: string;
  placeholder: string;
  icon: string;
  field: Accessor<string>;
  setField: Setter<string>;
}

export function RequiredHiddenInput(props: RequiredHiddenInputProps) {
  const errorMessage = createMemo(() => {
    const v = props.field();
    if (v.length === 0) {
      return `A value for ${props.label} is required.`;
    } else {
      return '';
    }
  });
  const [hidden, setHidden] = createSignal(true);

  return (
    <div class="mb-2 field is-horizontal">
      <div class="field-label is-normal">
        <label class="label" for={props.name}>
          {props.label}
        </label>
      </div>
      <div class="field-body">
        <div class="field is-expanded">
          <div class="field has-addons">
            <p class="control is-expanded has-icons-left">
              <input
                class="input"
                type={hidden() ? 'password' : 'text'}
                id={props.name}
                placeholder={props.placeholder}
                use:textField={[props.field, props.setField]}
                autocomplete="current-password"
              />
              <span class="icon is-small is-left">
                <i class={props.icon}></i>
              </span>
            </p>
            <p class="control">
              <button
                class="button"
                on:click={(ev) => {
                  // default action attempts to submit the form
                  ev.preventDefault();
                  setHidden((v) => !v);
                }}
              >
                <span class="icon is-small">
                  <i class={hidden() ? 'fas fa-eye' : 'fas fa-eye-slash'}></i>
                </span>
              </button>
            </p>
          </div>
          <Show when={errorMessage().length > 0}>
            <p class="help is-danger">{errorMessage()}</p>
          </Show>
        </div>
      </div>
    </div>
  );
}
