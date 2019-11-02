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

module LocalForm = {
  open Formality;

  type field =
    | Label
    | Basepath;

  type state = {
    label: string,
    basepath: string,
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

  module BasepathField = {
    let update = (state, value) => {...state, basepath: value};

    let validator = {
      field: Basepath,
      strategy: Strategy.OnFirstSuccessOrFirstBlur,
      dependents: None,
      validate: state =>
        switch (state.label) {
        | "" => Error("Please enter a path")
        | _ => Ok(Valid)
        },
    };
  };

  let validators = [LabelField.validator, BasepathField.validator];
};

module LocalFormHook = Formality.Make(LocalForm);

module SecureFtpForm = {
  open Formality;

  type field =
    | Label
    | Address
    | Username
    | Password
    | Basepath;

  type state = {
    label: string,
    remote_addr: string,
    username: string,
    password: option(string),
    basepath: option(string),
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

  module AddressField = {
    let update = (state, value) => {...state, remote_addr: value};

    let validator = {
      field: Address,
      strategy: Strategy.OnFirstSuccessOrFirstBlur,
      dependents: None,
      validate: state =>
        switch (state.label) {
        | "" => Error("Please enter the remote address")
        | _ => Ok(Valid)
        },
    };
  };

  module UsernameField = {
    let update = (state, value) => {...state, username: value};

    let validator = {
      field: Username,
      strategy: Strategy.OnFirstSuccessOrFirstBlur,
      dependents: None,
      validate: state =>
        switch (state.label) {
        | "" => Error("Please enter a user name")
        | _ => Ok(Valid)
        },
    };
  };

  module PasswordField = {
    let update = (state, value) =>
      switch (value) {
      | "" => {...state, password: None}
      | _ => {...state, password: Some(value)}
      };

    let validator = {
      field: Password,
      strategy: Strategy.OnFirstSuccessOrFirstBlur,
      dependents: None,
      validate: _state => Ok(Valid),
    };
  };

  module BasepathField = {
    let update = (state, value) =>
      switch (value) {
      | "" => {...state, basepath: None}
      | _ => {...state, basepath: Some(value)}
      };

    let validator = {
      field: Basepath,
      strategy: Strategy.OnFirstSuccessOrFirstBlur,
      dependents: None,
      validate: _state => Ok(Valid),
    };
  };

  let validators = [
    LabelField.validator,
    AddressField.validator,
    UsernameField.validator,
    PasswordField.validator,
    BasepathField.validator,
  ];
};

module SecureFtpFormHook = Formality.Make(SecureFtpForm);

module MinioForm = {
  open Formality;

  type field =
    | Label
    | Region
    | Endpoint;

  type state = {
    label: string,
    region: string,
    endpoint: string,
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

  module RegionField = {
    let update = (state, value) => {...state, region: value};

    let validator = {
      field: Region,
      strategy: Strategy.OnFirstSuccessOrFirstBlur,
      dependents: None,
      validate: state =>
        switch (state.region) {
        | "" => Error("Please enter a region")
        | _ => Ok(Valid)
        },
    };
  };

  module EndpointField = {
    let update = (state, value) => {...state, endpoint: value};

    let validator = {
      field: Endpoint,
      strategy: Strategy.OnFirstSuccessOrFirstBlur,
      dependents: None,
      validate: state =>
        switch (state.endpoint) {
        | "" => Error("Please enter an endpoint")
        | _ => Ok(Valid)
        },
    };
  };

  let validators = [
    LabelField.validator,
    RegionField.validator,
    EndpointField.validator,
  ];
};

module MinioFormHook = Formality.Make(MinioForm);

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
           form.state.basepath,
           "c:\\mystuff",
           validateMsg(Basepath),
           _ => form.blur(Basepath),
           event =>
             form.change(
               Basepath,
               LocalForm.BasepathField.update(
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

module SecureFtpFormRe = {
  [@react.component]
  let make = (~initial: SecureFtpForm.state, ~onSubmit, ~storeKey=None) => {
    let form: SecureFtpFormHook.interface =
      SecureFtpFormHook.useForm(
        ~initialState=initial, ~onSubmit=(state, _form) =>
        onSubmit(state)
      );
    let validateMsg = (field: SecureFtpForm.field) =>
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
           "SFTP Label",
           "label",
           "text",
           form.state.label,
           "Garage Server",
           validateMsg(Label),
           _ => form.blur(Label),
           event =>
             form.change(
               Label,
               SecureFtpForm.LabelField.update(
                 form.state,
                 event->ReactEvent.Form.target##value,
               ),
             ),
         )}
        {formInput(
           "Address",
           "remote_addr",
           "text",
           form.state.remote_addr,
           "192.168.1.3:2222",
           validateMsg(Address),
           _ => form.blur(Address),
           event =>
             form.change(
               Address,
               SecureFtpForm.AddressField.update(
                 form.state,
                 event->ReactEvent.Form.target##value,
               ),
             ),
         )}
        {formInput(
           "Username",
           "username",
           "text",
           form.state.username,
           "me",
           validateMsg(Username),
           _ => form.blur(Username),
           event =>
             form.change(
               Username,
               SecureFtpForm.UsernameField.update(
                 form.state,
                 event->ReactEvent.Form.target##value,
               ),
             ),
         )}
        {formInput(
           "Password",
           "password",
           "text",
           Belt.Option.getWithDefault(form.state.password, ""),
           "192.168.1.3:2222",
           validateMsg(Password),
           _ => form.blur(Password),
           event =>
             form.change(
               Password,
               SecureFtpForm.PasswordField.update(
                 form.state,
                 event->ReactEvent.Form.target##value,
               ),
             ),
         )}
        {formInput(
           "Remote Path",
           "basepath",
           "text",
           Belt.Option.getWithDefault(form.state.basepath, ""),
           "/home/me/packfiles",
           validateMsg(Basepath),
           _ => form.blur(Basepath),
           event =>
             form.change(
               Basepath,
               SecureFtpForm.BasepathField.update(
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

module MinioFormRe = {
  [@react.component]
  let make = (~initial: MinioForm.state, ~onSubmit, ~storeKey=None) => {
    let form: MinioFormHook.interface =
      MinioFormHook.useForm(~initialState=initial, ~onSubmit=(state, _form) =>
        onSubmit(state)
      );
    let validateMsg = (field: MinioForm.field) =>
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
           "Minio Label",
           "label",
           "text",
           form.state.label,
           "Bucket List",
           validateMsg(Label),
           _ => form.blur(Label),
           event =>
             form.change(
               Label,
               MinioForm.LabelField.update(
                 form.state,
                 event->ReactEvent.Form.target##value,
               ),
             ),
         )}
        {formInput(
           "Region",
           "region",
           "text",
           form.state.region,
           "us-west-1",
           validateMsg(Region),
           _ => form.blur(Region),
           event =>
             form.change(
               Region,
               MinioForm.RegionField.update(
                 form.state,
                 event->ReactEvent.Form.target##value,
               ),
             ),
         )}
        {formInput(
           "Endpoint",
           "endpoint",
           "text",
           form.state.endpoint,
           "http://192.168.1.3:9000",
           validateMsg(Endpoint),
           _ => form.blur(Endpoint),
           event =>
             form.change(
               Endpoint,
               MinioForm.EndpointField.update(
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

module type Store = {
  type state;
  let initial: unit => state;
  let kind: unit => string;
  let decode: Js.Json.t => state;
  let encode: state => Js.Json.t;
  let component: (state, state => unit, option(string)) => React.element;
};

module LocalStore: Store = {
  type state = LocalForm.state;
  let initial = (): state => {label: "", basepath: ""};
  let kind = (): string => "local";
  let decode = (json: Js.Json.t): state =>
    Json.Decode.{
      label: json |> field("label", string),
      basepath: json |> field("basepath", string),
    };
  let encode = (self: state): Js.Json.t =>
    Json.Encode.(
      object_([
        ("label", string(self.label)),
        ("basepath", string(self.basepath)),
      ])
    );
  let component = (initial, onSubmit, storeKey) =>
    <LocalFormRe initial onSubmit storeKey />;
};

module SecureFtpStore: Store = {
  type state = SecureFtpForm.state;
  let initial = (): state => {
    label: "",
    remote_addr: "",
    username: "",
    password: None,
    basepath: None,
  };
  let kind = (): string => "sftp";
  let decode = (json: Js.Json.t): state =>
    Json.Decode.{
      label: json |> field("label", string),
      remote_addr: json |> field("remote_addr", string),
      username: json |> field("username", string),
      password: json |> optional(field("password", string)),
      basepath: json |> optional(field("basepath", string)),
    };
  let encode = (self: state): Js.Json.t =>
    Json.Encode.(
      object_([
        ("label", string(self.label)),
        ("remote_addr", string(self.remote_addr)),
        ("username", string(self.username)),
        (
          "password",
          switch (self.password) {
          | Some(value) => string(value)
          | None => null
          },
        ),
        (
          "basepath",
          switch (self.basepath) {
          | Some(value) => string(value)
          | None => null
          },
        ),
      ])
    );
  let component = (initial, onSubmit, storeKey) =>
    <SecureFtpFormRe initial onSubmit storeKey />;
};

module MinioStore: Store = {
  type state = MinioForm.state;
  let initial = (): state => {label: "", region: "", endpoint: ""};
  let kind = (): string => "minio";
  let decode = (json: Js.Json.t): state =>
    Json.Decode.{
      label: json |> field("label", string),
      region: json |> field("region", string),
      endpoint: json |> field("endpoint", string),
    };
  let encode = (self: state): Js.Json.t =>
    Json.Encode.(
      object_([
        ("label", string(self.label)),
        ("region", string(self.region)),
        ("endpoint", string(self.endpoint)),
      ])
    );
  let component = (initial, onSubmit, storeKey) =>
    <MinioFormRe initial onSubmit storeKey />;
};

module MakeNewWidget = (Item: Store) => {
  let submitNewStore =
      (mutate: DefineStoreMutation.apolloMutation, values: Item.state) => {
    let options: Js.Json.t = Item.encode(values);
    let text = Json.stringify(options);
    let update =
      DefineStore.make(~typeName=Item.kind(), ~options=btoa(text), ());
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
          Item.component(Item.initial(), submitNewStore(mutate), None);
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

module LocalNewWidget = MakeNewWidget(LocalStore);
module SecureFtpNewWidget = MakeNewWidget(SecureFtpStore);
module MinioNewWidget = MakeNewWidget(MinioStore);

module MakeEditWidget = (Item: Store) => {
  let submitEditStore =
      (
        mutate: UpdateStoreMutation.apolloMutation,
        key: string,
        values: Item.state,
      ) => {
    let options: Js.Json.t = Item.encode(values);
    let text = Json.stringify(options);
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
    store##options |> atob |> Json.parseOrRaise |> Item.decode;
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
          Item.component(
            initial,
            submitEditStore(mutate, store##key),
            Some(store##key),
          );
        | NotCalled =>
          let initial = computeInitial(store);
          Item.component(
            initial,
            submitEditStore(mutate, store##key),
            Some(store##key),
          );
        }
      }
    </UpdateStoreMutation>;
  };
};

module LocalEditWidget = MakeEditWidget(LocalStore);
module SecureFtpEditWidget = MakeEditWidget(SecureFtpStore);
module MinioEditWidget = MakeEditWidget(MinioStore);

let editorForStore = (store: t) =>
  switch (store##kind) {
  | "local" => <LocalEditWidget store />
  | "sftp" => <SecureFtpEditWidget store />
  | "minio" => <MinioEditWidget store />
  | _ =>
    Js.log2("got an unsupported store type:", store##kind);
    React.null;
  };

module DeleteStoreWidget = {
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

module NewPanel = {
  [@react.component]
  let make = () => {
    let (kind, setKind) = React.useState(() => "Local");
    <div>
      <form>
        <div
          className="container"
          style={ReactDOMRe.Style.make(
            ~width="auto",
            ~paddingRight="6em",
            (),
          )}>
          <div
            className="field is-horizontal"
            key="kind"
            // since this field _is_ the last child, need to
            // add the padding that bulma would have added
            style={ReactDOMRe.Style.make(
              ~paddingTop=".75em",
              ~paddingBottom=".75em",
              (),
            )}>
            <div className="field-label is-normal">
              <label htmlFor="kind" className="label">
                {ReasonReact.string("Store Kind")}
              </label>
            </div>
            <div className="field-body">
              <div className="field">
                <div className="control" key="the_control">
                  <div className="select">
                    <select
                      id="kind"
                      name="kind"
                      onChange={event =>
                        setKind(event->ReactEvent.Form.target##value)
                      }>
                      <option> {ReasonReact.string("Local")} </option>
                      <option> {ReasonReact.string("SFTP")} </option>
                      <option> {ReasonReact.string("Minio")} </option>
                    </select>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </form>
      {switch (kind) {
       | "Local" => <LocalNewWidget />
       | "SFTP" => <SecureFtpNewWidget />
       | "Minio" => <MinioNewWidget />
       | _ =>
         Js.log2("unsupported store type:", kind);
         React.null;
       }}
    </div>;
  };
};

module Component = {
  [@react.component]
  let make = () => {
    let buildEditWidgets = (stores: array(t)) =>
      Array.map(
        (store: t) =>
          <div
            key={
              store##key;
            }>
            <DeleteStoreWidget store />
            {editorForStore(store)}
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
            {ReasonReact.array(buildEditWidgets(data##stores))}
            <p>
              {React.string("Use the form below to add a new pack store.")}
            </p>
            <NewPanel />
          </div>
        }
      }
    </GetStoresQuery>;
  };
};