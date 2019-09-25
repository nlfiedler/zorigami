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

type local = {
  label: string,
  basepath: string,
};

type minio = {
  label: string,
  region: string,
  endpoint: string,
};

type sftp = {
  label: string,
  remote_addr: string,
  username: string,
  password: option(string),
  basepath: option(string),
};

module Decode = {
  let local = (json: Js.Json.t): local =>
    Json.Decode.{
      label: json |> field("label", string),
      basepath: json |> field("basepath", string),
    };

  let minio = (json: Js.Json.t): minio =>
    Json.Decode.{
      label: json |> field("label", string),
      region: json |> field("region", string),
      endpoint: json |> field("endpoint", string),
    };

  let sftp = (json: Js.Json.t): sftp =>
    Json.Decode.{
      label: json |> field("label", string),
      remote_addr: json |> field("remote_addr", string),
      username: json |> field("username", string),
      password: json |> optional(field("password", string)),
      basepath: json |> optional(field("basepath", string)),
    };
};

module Encode = {
  let local = (opts: local) =>
    Json.Encode.(
      object_([
        ("label", string(opts.label)),
        ("basepath", string(opts.basepath)),
      ])
    );

  let minio = (opts: minio) =>
    Json.Encode.(
      object_([
        ("label", string(opts.label)),
        ("region", string(opts.region)),
        ("endpoint", string(opts.endpoint)),
      ])
    );

  let sftp = (opts: sftp) =>
    Json.Encode.(
      object_([
        ("label", string(opts.label)),
        ("remote_addr", string(opts.remote_addr)),
        ("username", string(opts.username)),
        (
          "password",
          switch (opts.password) {
          | Some(value) => string(value)
          | None => null
          },
        ),
        (
          "basepath",
          switch (opts.basepath) {
          | Some(value) => string(value)
          | None => null
          },
        ),
      ])
    );
};

module LocalForm = {
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

module LocalFormHook = Formality.Make(LocalForm);

// Read-only form input for displaying a value.
let formDisplay =
    (
      labelText: string,
      inputId: string,
      inputType: string,
      inputValue: string,
    ) => {
  let inputField =
    <div className="control" key="the_control">
      <input
        id=inputId
        className="input"
        type_=inputType
        name=inputId
        value=inputValue
        readOnly=true
      />
    </div>;
  <div className="field is-horizontal" key=inputId>
    <div className="field-label is-normal">
      <label htmlFor=inputId className="label">
        {ReasonReact.string(labelText)}
      </label>
    </div>
    <div className="field-body">
      <div className="field"> inputField </div>
    </div>
  </div>;
};

let formInput =
    (
      labelText: string,
      inputId: string,
      inputType: string,
      inputValue: string,
      placeholderText: string,
      validateMsg: string,
      onBlur,
      onChange,
    ) => {
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
        onBlur
        onChange
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

let assetDeleteButton = (storeKey: option(string)) =>
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

// alias for the private form status type in Formality.Form
type formStatus =
  | Submitting
  | SubmissionFailed
  | SomethingElse;

let assetSaveButton = (status: formStatus, storeKey: option(string)) =>
  switch (status) {
  | Submitting => <p> {React.string("Saving...")} </p>
  | SubmissionFailed =>
    <div className="field is-grouped">
      <p className="control">
        <input type_="submit" value="Save" className="button" disabled=true />
      </p>
      {assetDeleteButton(storeKey)}
    </div>
  | _ =>
    <div className="field is-grouped">
      <p className="control">
        <input type_="submit" value="Save" className="button is-primary" />
      </p>
      {assetDeleteButton(storeKey)}
    </div>
  };

module LocalFormRe = {
  [@react.component]
  let make = (~initial: LocalForm.state, ~onSubmit, ~storeKey=None) => {
    let form: LocalFormHook.interface =
      LocalFormHook.useForm(~initialState=initial, ~onSubmit=(state, _form) =>
        onSubmit(state)
      );
    let validateMsg = (field: LocalForm.field) =>
      switch (form.result(field)) {
      | Some(Error(message)) => message
      | Some(Ok(Valid | NoValue))
      | None => ""
      };
    <form onSubmit={form.submit->Formality.Dom.preventDefault}>
      <div
        className="container"
        style={ReactDOMRe.Style.make(~width="auto", ~paddingRight="6em", ())}>
        {formInput(
           "Local Label",
           "label",
           "text",
           form.state.label,
           "My Stuff",
           validateMsg(Label),
           _ => form.blur(Label),
           event =>
             form.change(
               Label,
               LocalForm.LabelField.update(
                 form.state,
                 event->ReactEvent.Form.target##value,
               ),
             ),
         )}
        {formInput(
           "Local Path",
           "basepath",
           "text",
           form.state.options,
           "c:\\mystuff",
           validateMsg(Options),
           _ => form.blur(Options),
           event =>
             form.change(
               Options,
               LocalForm.OptionsField.update(
                 form.state,
                 event->ReactEvent.Form.target##value,
               ),
             ),
         )}
        {switch (storeKey) {
         | Some(key) => formDisplay("Store Key", "storekey", "text", key)
         | None => React.null
         }}
        <div className="field is-horizontal">
          <div className="field-label" />
          <div className="field-body">
            {switch (form.status) {
             | Submitting(_) => assetSaveButton(Submitting, storeKey)
             | SubmissionFailed(_) =>
               assetSaveButton(SubmissionFailed, storeKey)
             | _ => assetSaveButton(SomethingElse, storeKey)
             }}
          </div>
        </div>
      </div>
    </form>;
  };
};

module NewStorePanel = {
  let submitNewStore =
      (mutate: DefineStoreMutation.apolloMutation, values: LocalForm.state) => {
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
          <LocalFormRe
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
        values: LocalForm.state,
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
    let initial: LocalForm.state = {
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
          <LocalFormRe
            initial
            onSubmit={submitEditStore(mutate, store##key)}
            storeKey={Some(store##key)}
          />;
        | NotCalled =>
          let initial = computeInitial(store);
          <LocalFormRe
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
        | Data(_result) => React.null
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
            <p>
              {React.string("Use the form below to add a new pack store.")}
            </p>
            <NewStorePanel />
          </div>
        }
      }
    </GetStoresQuery>;
  };
};