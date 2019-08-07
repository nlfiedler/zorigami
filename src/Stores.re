//
// Copyright (c) 2019 Nathan Fiedler
//
[@bs.val] external btoa: string => string = "btoa"; // encode
[@bs.val] external atob: string => string = "atob"; // decode

// The expected shape of the stores from GraphQL.
type t = {
  .
  "key": string,
  "label": string,
  "kind": string,
  "options": string,
};

module GetStores = [%graphql
  {|
    query getAllStores {
      stores {
        key
        label
        kind
        options
      }
    }
  |}
];

module GetStoresQuery = ReasonApollo.CreateQuery(GetStores);

/*
 * Have the response include all of the fields that the user can modify,
 * that way the Apollo Client will automatically update the cached values.
 */
module DefineStore = [%graphql
  {|
    mutation DefineStore($typeName: String!, $options: String!) {
      defineStore(typeName: $typeName, options: $options) {
        key
        label
        kind
        options
      }
    }
  |}
];

module DefineStoreMutation = ReasonApollo.CreateMutation(DefineStore);

/*
 * Have the response include all of the fields that the user can modify,
 * that way the Apollo Client will automatically update the cached values.
 */
module UpdateStore = [%graphql
  {|
    mutation UpdateStore($key: String!, $options: String!) {
      updateStore(key: $key, options: $options) {
        key
        label
        kind
        options
      }
    }
  |}
];

module UpdateStoreMutation = ReasonApollo.CreateMutation(UpdateStore);

module DeleteStore = [%graphql
  {|
    mutation DeleteStore($key: String!) {
      deleteStore(key: $key) {
        key
      }
    }
  |}
];

module DeleteStoreMutation = ReasonApollo.CreateMutation(DeleteStore);

// type of the local store options
type local = {
  label: string,
  basepath: string,
};

module Decode = {
  let local = json =>
    Json.Decode.{
      label: json |> field("label", string),
      basepath: json |> field("basepath", string),
    };
};

module Encode = {
  let local = opts =>
    Json.Encode.(
      object_([
        ("label", string(opts.label)),
        ("basepath", string(opts.basepath)),
      ])
    );
};

module StoreForm = {
  open Formality;

  type field =
    | Label
    | Options;

  type state = {
    label: string,
    options: string,
  };

  type message = string;
  type submissionError = unit;
  // define this updater type for convenience
  type updater = (state, string) => state;

  module LabelField = {
    let update = (state, value) => {...state, label: value};

    let validator = {
      field: Label,
      strategy: Strategy.OnFirstSuccessOrFirstBlur,
      dependents: None,
      validate: state =>
        switch (state.label) {
        | "" => Error("Please enter a label")
        | _ => Ok(Valid)
        },
    };
  };

  module OptionsField = {
    let update = (state, value) => {...state, options: value};

    let validator = {
      field: Label,
      strategy: Strategy.OnFirstSuccessOrFirstBlur,
      dependents: None,
      validate: _state => Ok(Valid),
    };
  };

  let validators = [LabelField.validator, OptionsField.validator];
};

module StoreFormHook = Formality.Make(StoreForm);

let formInput =
    (
      form: StoreFormHook.interface,
      field: StoreForm.field,
      updater: StoreForm.updater,
      labelText: string,
      inputId: string,
      inputType: string,
      inputValue: string,
      placeholderText: string,
      readOnly: bool,
    ) => {
  let validateMsg =
    switch (form.result(field)) {
    | Some(Error(message)) => message
    | Some(Ok(Valid | NoValue))
    | None => ""
    };
  let formIsValid = validateMsg == "";
  let inputClass = formIsValid ? "input" : "input is-danger";
  let validationTextDiv =
    <p className="help is-danger" key="the_message">
      {ReasonReact.string(validateMsg)}
    </p>;
  let inputField =
    <div className="control" key="the_control">
      <input
        id=inputId
        className=inputClass
        type_=inputType
        name=inputId
        value=inputValue
        onBlur={_ => form.blur(field)}
        onChange={event =>
          form.change(
            field,
            updater(form.state, event->ReactEvent.Form.target##value),
          )
        }
        readOnly
        placeholder=placeholderText
      />
    </div>;
  let formGroupElems =
    if (formIsValid) {
      inputField;
    } else {
      ReasonReact.array([|inputField, validationTextDiv|]);
    };
  <div className="field is-horizontal" key=inputId>
    <div className="field-label is-normal">
      <label htmlFor=inputId className="label">
        {ReasonReact.string(labelText)}
      </label>
    </div>
    <div className="field-body">
      <div className="field"> formGroupElems </div>
    </div>
  </div>;
};

let deleteFormName = (key: string) => {
  "deleteForm_" ++ key;
};

module StoreFormRe = {
  [@react.component]
  // if storeKey is None, form will be in edit mode w/o cancel button
  let make = (~initial: StoreForm.state, ~onSubmit, ~storeKey=None) => {
    let newform = Belt.Option.isNone(storeKey);
    let (editing, setEditing) = React.useState(() => newform);
    let form: StoreFormHook.interface =
      StoreFormHook.useForm(~initialState=initial, ~onSubmit=(state, _form) =>
        onSubmit(state)
      );
    let cancelLink =
      newform
        ? React.null
        : <p className="control">
            <a
              href="#"
              className="button is-text"
              onClick={_ => {
                setEditing(_ => false);
                form.reset();
              }}
              title="Cancel">
              {React.string("Cancel")}
            </a>
          </p>;
    let assetSaveButton = () =>
      switch (form.status) {
      | Submitting(_) => <p> {React.string("Saving...")} </p>
      | SubmissionFailed(_) =>
        <div className="field is-grouped">
          <p className="control">
            <input
              type_="submit"
              value="Save"
              className="button"
              disabled=true
            />
          </p>
          cancelLink
        </div>
      | _ =>
        <div className="field is-grouped">
          <p className="control">
            <input type_="submit" value="Save" className="button is-primary" />
          </p>
          cancelLink
        </div>
      };
    let assetDeleteButton =
      switch (storeKey) {
      | Some(key) =>
        <p className="control">
          <input
            type_="submit"
            value="Delete"
            className="button is-danger is-outlined"
            form={deleteFormName(key)}
          />
        </p>
      | None => React.null
      };
    let assetEditButton =
      <div className="field is-grouped">
        <p className="control">
          <a
            onClick={_ => setEditing(_ => true)}
            href="#"
            title="Edit"
            className="button is-primary">
            {React.string("Edit")}
          </a>
        </p>
        assetDeleteButton
      </div>;
    <form onSubmit={form.submit->Formality.Dom.preventDefault}>
      <div
        className="container"
        style={ReactDOMRe.Style.make(~width="auto", ~paddingRight="6em", ())}>
        {newform
           ? <div className="field is-horizontal" key="help_text">
               <div className="field-label is-normal" />
               <div className="field-body">
                 <div className="field">
                   {React.string(
                      "Use the form below to add a new pack store.",
                    )}
                 </div>
               </div>
             </div>
           : React.null}
        {formInput(
           form,
           Label,
           StoreForm.LabelField.update,
           "Label",
           "label",
           "text",
           form.state.label,
           "My Stuff",
           !editing,
         )}
        {formInput(
           form,
           Options,
           StoreForm.OptionsField.update,
           "Path",
           "basepath",
           "text",
           form.state.options,
           "c:\\mystuff",
           !editing,
         )}
        <div className="field is-horizontal">
          <div className="field-label" />
          <div className="field-body">
            {editing ? assetSaveButton() : assetEditButton}
          </div>
        </div>
      </div>
    </form>;
  };
};

module NewStorePanel = {
  let submitNewStore =
      (mutate: DefineStoreMutation.apolloMutation, values: StoreForm.state) => {
    let options: local = {label: values.label, basepath: values.options};
    let text = Json.stringify(Encode.local(options));
    let update =
      DefineStore.make(~typeName="local", ~options=btoa(text), ());
    // ignore the returned promise, the result will be delivered later
    mutate(
      ~variables=update##variables,
      ~refetchQueries=[|"getAllStores"|],
      (),
    )
    |> ignore;
  };
  [@react.component]
  let make = () => {
    <DefineStoreMutation>
      ...{(mutate, {result}) => {
        let storeForm =
          <StoreFormRe
            initial={label: "", options: ""}
            onSubmit={submitNewStore(mutate)}
          />;
        switch (result) {
        | Loading => <p> {ReasonReact.string("Preparing...")} </p>
        | Error(error) =>
          Js.log(error);
          <div> {ReasonReact.string(error##message)} </div>;
        | Data(_result) => storeForm
        | NotCalled => storeForm
        };
      }}
    </DefineStoreMutation>;
  };
};

module EditStorePanel = {
  let submitEditStore =
      (
        mutate: UpdateStoreMutation.apolloMutation,
        key: string,
        values: StoreForm.state,
      ) => {
    let options: local = {label: values.label, basepath: values.options};
    let text = Json.stringify(Encode.local(options));
    let update = UpdateStore.make(~key, ~options=btoa(text), ());
    // ignore the returned promise, the result will be delivered later
    mutate(
      ~variables=update##variables,
      ~refetchQueries=[|"getAllStores"|],
      (),
    )
    |> ignore;
  };
  let computeInitial = (store: t) => {
    let options = store##options |> atob |> Json.parseOrRaise |> Decode.local;
    let initial: StoreForm.state = {
      label: options.label,
      options: options.basepath,
    };
    initial;
  };
  [@react.component]
  let make = (~store: t) => {
    <UpdateStoreMutation>
      ...{(mutate, {result}) =>
        switch (result) {
        | Loading => <p> {ReasonReact.string("Saving the store...")} </p>
        | Error(error) =>
          Js.log(error);
          <div> {ReasonReact.string(error##message)} </div>;
        | Data(result) =>
          let initial = computeInitial(result##updateStore);
          <StoreFormRe
            initial
            onSubmit={submitEditStore(mutate, store##key)}
            storeKey={Some(store##key)}
          />;
        | NotCalled =>
          let initial = computeInitial(store);
          <StoreFormRe
            initial
            onSubmit={submitEditStore(mutate, store##key)}
            storeKey={Some(store##key)}
          />;
        }
      }
    </UpdateStoreMutation>;
  };
};

module DeleteStorePanel = {
  let submitDeleteStore =
      (mutate: DeleteStoreMutation.apolloMutation, key: string) => {
    let update = DeleteStore.make(~key, ());
    // ignore the returned promise, the result will be delivered later
    mutate(
      ~variables=update##variables,
      ~refetchQueries=[|"getAllStores"|],
      (),
    )
    |> ignore;
  };
  [@react.component]
  let make = (~store: t) => {
    <DeleteStoreMutation>
      ...{(mutate, {result}) =>
        switch (result) {
        | Loading => <p> {ReasonReact.string("Deleting the store...")} </p>
        | Error(error) =>
          Js.log(error);
          <div> {ReasonReact.string(error##message)} </div>;
        | Data(result) =>
          Js.log2("deleted store:", result##deleteStore##key);
          React.null;
        | NotCalled =>
          <form
            onSubmit={_ => submitDeleteStore(mutate, store##key)}
            id={deleteFormName(store##key)}
          />
        }
      }
    </DeleteStoreMutation>;
  };
};

module Component = {
  [@react.component]
  let make = () => {
    let buildEditPanels = (stores: array(t)) =>
      Array.map(
        (store: t) =>
          <div
            key={
              store##key;
            }>
            <DeleteStorePanel store />
            <EditStorePanel store />
            <hr />
          </div>,
        stores,
      );
    <GetStoresQuery>
      ...{({result}) =>
        switch (result) {
        | Loading =>
          <div> {ReasonReact.string("Loading the stores...")} </div>
        | Error(error) =>
          Js.log(error);
          <div> {ReasonReact.string(error##message)} </div>;
        | Data(data) =>
          <div>
            {ReasonReact.array(buildEditPanels(data##stores))}
            <NewStorePanel />
          </div>
        }
      }
    </GetStoresQuery>;
  };
};