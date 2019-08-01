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

module StoreLenses = [%lenses
  type state = {
    label: string,
    options: string,
  }
];

module StoreForm = ReFormNext.Make(StoreLenses);

let formInput =
    (
      handleChange,
      fieldState: ReFormNext.fieldState,
      fieldTitle: StoreLenses.field('a),
      labelText: string,
      inputId: string,
      inputType: string,
      inputValue: string,
      placeholderText: string,
      readOnly: bool,
    ) => {
  // would invoke getFieldState(Field(<label>)) here but Field() is not in scope
  let validateMsg =
    fieldState
    |> (
      fun
      | Error(error) => Some(error)
      | _ => None
    )
    |> Belt.Option.getWithDefault(_, "");
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
        onChange={ReForm.Helpers.handleDomFormChange(
          handleChange(fieldTitle),
        )}
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

module StoreFormRe = {
  [@react.component]
  // set newform=true to have form in edit mode w/o cancel button
  let make = (~initial: StoreLenses.state, ~onSubmit, ~newform=false) => {
    let (editing, setEditing) = React.useState(() => newform);
    let {state, submit, getFieldState, handleChange, resetForm}: StoreForm.api =
      StoreForm.use(
        ~schema={
          StoreForm.Validation.Schema([|
            Custom(
              Label,
              values =>
                values.label == "" ? Error("Please enter a label") : Valid,
            ),
          |]);
        },
        ~onSubmit=({state}) => onSubmit(state.values),
        ~initialState=initial,
        (),
      );
    let cancelLink =
      newform
        ? <div />
        : <a
            href="#"
            className="button is-text"
            onClick={_ => {
              setEditing(_ => false);
              resetForm();
            }}
            title="Cancel">
            {React.string("Cancel")}
          </a>;
    let assetSaveButton = (state: StoreForm.state) =>
      switch (state.formState) {
      | Submitting => <p> {React.string("Saving...")} </p>
      | Errored =>
        <div className="field is-grouped">
          <input
            type_="submit"
            value="Save"
            className="button"
            disabled=true
          />
          cancelLink
        </div>
      | _ =>
        <div className="field is-grouped">
          <input type_="submit" value="Save" className="button is-primary" />
          cancelLink
        </div>
      };
    let assetEditButton =
      <a
        onClick={_ => setEditing(_ => true)}
        href="#"
        title="Edit"
        className="button is-primary">
        {React.string("Edit")}
      </a>;
    <form
      onSubmit={event => {
        ReactEvent.Synthetic.preventDefault(event);
        submit();
      }}>
      <div
        className="container"
        style={ReactDOMRe.Style.make(~width="auto", ~paddingRight="6em", ())}>
        {formInput(
           handleChange,
           getFieldState(Field(Label)),
           Label,
           "Label",
           "label",
           "text",
           state.values.label,
           "My Stuff",
           !editing,
         )}
        {formInput(
           handleChange,
           getFieldState(Field(Options)),
           Options,
           "Path",
           "basepath",
           "text",
           state.values.options,
           "c:\\mystuff",
           !editing,
         )}
        <div className="field is-horizontal">
          <div className="field-label" />
          <div className="field-body">
            {editing ? assetSaveButton(state) : assetEditButton}
          </div>
        </div>
      </div>
    </form>;
  };
};

module NewStorePanel = {
  let submitNewStore =
      (mutate: DefineStoreMutation.apolloMutation, values: StoreLenses.state) => {
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
    // bs-reform expects an option return value for onSubmit
    None;
  };
  [@react.component]
  let make = () => {
    <DefineStoreMutation>
      ...{(mutate, {result}) => {
        let storeForm =
          <StoreFormRe
            initial={label: "", options: ""}
            onSubmit={submitNewStore(mutate)}
            newform=true
          />;
        switch (result) {
        | Loading => <p> {ReasonReact.string("Loading...")} </p>
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
        values: StoreLenses.state,
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
    // bs-reform expects an option return value for onSubmit
    None;
  };
  let computeInitial = (store: t) => {
    let options = store##options |> atob |> Json.parseOrRaise |> Decode.local;
    let initial: StoreLenses.state = {
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
        | Loading => <p> {ReasonReact.string("Loading...")} </p>
        | Error(error) =>
          Js.log(error);
          <div> {ReasonReact.string(error##message)} </div>;
        | Data(result) =>
          let initial = computeInitial(result##updateStore);
          <StoreFormRe
            initial
            onSubmit={submitEditStore(mutate, store##key)}
          />;
        | NotCalled =>
          let initial = computeInitial(store);
          <StoreFormRe
            initial
            onSubmit={submitEditStore(mutate, store##key)}
          />;
        }
      }
    </UpdateStoreMutation>;
  };
};

module Component = {
  [@react.component]
  let make = () => {
    let buildEditPanels = (stores: array(t)) =>
      Array.map(
        (store: t) =>
          <div> <EditStorePanel key={store##key} store /> <hr /> </div>,
        stores,
      );
    <GetStoresQuery>
      ...{({result}) =>
        switch (result) {
        | Loading => <div> {ReasonReact.string("Loading...")} </div>
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