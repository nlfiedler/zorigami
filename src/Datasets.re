//
// Copyright (c) 2019 Nathan Fiedler
//

// The expected shape of the datasets from GraphQL.
type t = {
  .
  "key": string,
  "computerId": string,
  "basepath": string,
  "schedule": option(string),
  "packSize": Js.Json.t,
  "stores": Js.Array.t(string),
};

module GetDatasets = [%graphql
  {|
    query getAllDatasets {
      datasets {
        key
        computerId
        basepath
        schedule
        packSize
        stores
      }
    }
  |}
];

module GetDatasetsQuery = ReasonApollo.CreateQuery(GetDatasets);

type input = {
  .
  "key": option(string),
  "basepath": string,
  "schedule": option(string),
  "packSize": Js.Json.t,
  "stores": Js.Array.t(string),
};

/*
 * Have the response include all of the fields that the user can modify,
 * that way the Apollo Client will automatically update the cached values.
 */
module DefineDataset = [%graphql
  {|
    mutation DefineDataset($dataset: InputDataset!) {
      defineDataset(dataset: $dataset) {
        key
        computerId
        basepath
        schedule
        packSize
        stores
      }
    }
  |}
];

module DefineDatasetMutation = ReasonApollo.CreateMutation(DefineDataset);

/*
 * Have the response include all of the fields that the user can modify,
 * that way the Apollo Client will automatically update the cached values.
 */
module UpdateDataset = [%graphql
  {|
    mutation UpdateDataset($dataset: InputDataset!) {
      updateDataset(dataset: $dataset) {
        key
        computerId
        basepath
        schedule
        packSize
        stores
      }
    }
  |}
];

module UpdateDatasetMutation = ReasonApollo.CreateMutation(UpdateDataset);

module DeleteDataset = [%graphql
  {|
    mutation DeleteDataset($key: String!) {
      deleteDataset(key: $key) {
        key
      }
    }
  |}
];

module DeleteDatasetMutation = ReasonApollo.CreateMutation(DeleteDataset);

module DatasetForm = {
  open Formality;

  type field =
    | Basepath
    | Schedule
    | PackSize
    | Stores;

  type state = {
    basepath: string,
    schedule: string, // option(string)
    pack_size: string, // Js.Json.t
    stores: string // Js.Array.t(string)
  };

  type message = string;
  type submissionError = unit;
  // define this updater type for convenience
  type updater = (state, string) => state;

  module BasepathField = {
    let update = (state, value) => {...state, basepath: value};

    let validator = {
      field: Basepath,
      strategy: Strategy.OnFirstSuccessOrFirstBlur,
      dependents: None,
      validate: state =>
        switch (state.basepath) {
        | "" => Error("Please enter a basepath")
        | _ => Ok(Valid)
        },
    };
  };

  module ScheduleField = {
    let update = (state, value) => {...state, schedule: value};

    let validator = {
      field: Schedule,
      strategy: Strategy.OnFirstSuccessOrFirstBlur,
      dependents: None,
      validate: state =>
        if (state.schedule == "") {
          Ok(Valid);
        } else {
          switch (state.schedule) {
          | "@hourly"
          | "@daily" => Ok(Valid)
          | _ => Error("Please enter @hourly, @daily, or nothing")
          };
        },
    };
  };

  module PackSizeField = {
    let update = (state, value) => {...state, pack_size: value};

    let validator = {
      field: PackSize,
      strategy: Strategy.OnFirstSuccessOrFirstBlur,
      dependents: None,
      validate: state =>
        if (state.pack_size == "") {
          Ok(Valid);
        } else {
          let result =
            try (int_of_string(state.pack_size)) {
            | Failure(_) => (-1)
            };
          if (result < 16 || result > 256) {
            Error("Please enter an value between 16 and 256");
          } else {
            Ok(Valid);
          };
        },
    };
  };

  module StoresField = {
    let update = (state, value) => {...state, stores: value};

    let validator = {
      field: Stores,
      strategy: Strategy.OnFirstSuccessOrFirstBlur,
      dependents: None,
      validate: state =>
        switch (state.stores) {
        | "" => Error("Please enter a store key")
        | _ => Ok(Valid)
        },
    };
  };

  let validators = [
    BasepathField.validator,
    ScheduleField.validator,
    PackSizeField.validator,
    StoresField.validator,
  ];
};

module DatasetFormHook = Formality.Make(DatasetForm);

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

let assetDeleteButton = (datasetKey: option(string)) =>
  switch (datasetKey) {
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

let assetSaveButton = (status: formStatus, datasetKey: option(string)) =>
  switch (status) {
  | Submitting => <p> {React.string("Saving...")} </p>
  | SubmissionFailed =>
    <div className="field is-grouped">
      <p className="control">
        <input type_="submit" value="Save" className="button" disabled=true />
      </p>
      {assetDeleteButton(datasetKey)}
    </div>
  | _ =>
    <div className="field is-grouped">
      <p className="control">
        <input type_="submit" value="Save" className="button is-primary" />
      </p>
      {assetDeleteButton(datasetKey)}
    </div>
  };

module DatasetFormRe = {
  [@react.component]
  let make = (~initial: DatasetForm.state, ~onSubmit, ~datasetKey=None) => {
    let form: DatasetFormHook.interface =
      DatasetFormHook.useForm(~initialState=initial, ~onSubmit=(state, _form) =>
        onSubmit(state)
      );
    let validateMsg = (field: DatasetForm.field) =>
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
           "Base Path",
           "basepath",
           "text",
           form.state.basepath,
           "c:\\mystuff",
           validateMsg(Basepath),
           _ => form.blur(Basepath),
           event =>
             form.change(
               Basepath,
               DatasetForm.BasepathField.update(
                 form.state,
                 event->ReactEvent.Form.target##value,
               ),
             ),
         )}
        {formInput(
           "Schedule",
           "schedule",
           "text",
           form.state.schedule,
           "@daily",
           validateMsg(Schedule),
           _ => form.blur(Schedule),
           event =>
             form.change(
               Schedule,
               DatasetForm.ScheduleField.update(
                 form.state,
                 event->ReactEvent.Form.target##value,
               ),
             ),
         )}
        {formInput(
           "Pack Size",
           "packsize",
           "number",
           form.state.pack_size,
           "64 (mb)",
           validateMsg(PackSize),
           _ => form.blur(PackSize),
           event =>
             form.change(
               PackSize,
               DatasetForm.PackSizeField.update(
                 form.state,
                 event->ReactEvent.Form.target##value,
               ),
             ),
         )}
        {formInput(
           "Pack Store",
           "stores",
           "text",
           form.state.stores,
           "store/local/xyz123",
           validateMsg(Stores),
           _ => form.blur(Stores),
           event =>
             form.change(
               Stores,
               DatasetForm.StoresField.update(
                 form.state,
                 event->ReactEvent.Form.target##value,
               ),
             ),
         )}
        <div className="field is-horizontal">
          <div className="field-label" />
          <div className="field-body">
            {switch (form.status) {
             | Submitting(_) => assetSaveButton(Submitting, datasetKey)
             | SubmissionFailed(_) =>
               assetSaveButton(SubmissionFailed, datasetKey)
             | _ => assetSaveButton(SomethingElse, datasetKey)
             }}
          </div>
        </div>
      </div>
    </form>;
  };
};

/*
 * Convert the input pack size value into a JSON string.
 *
 * Converts the value from megabytes to bytes which the backend expects.
 */
let packsizeToString = (str: string): Js.Json.t =>
  if (String.length(str) > 0) {
    let result =
      try (int_of_string(str)) {
      | Failure(_) => 0
      };
    let value = result * 1048576;
    Js.Json.string(string_of_int(value));
  } else {
    Js.Json.string("0");
  };

/*
 * Split the string on commas, replacing None with empty string.
 */
let stringToArray = (str: string): array(string) => {
  let parts = Js.String.splitByRe([%bs.re "/,/"], str);
  let splitStores = Array.map(a => Belt.Option.getWithDefault(a, ""), parts);
  /* this may introduce a single blank tag, but it's easier to let the backend prune it */
  Array.map(s => String.trim(s), splitStores);
};

/**
 * Convert an empty string to a None, any other value is Some(value).
 */
let stringToOption = (str: string): option(string) =>
  if (String.length(str) > 0) {
    Some(str);
  } else {
    None;
  };

module NewDatasetPanel = {
  let submitNewDataset =
      (
        mutate: DefineDatasetMutation.apolloMutation,
        values: DatasetForm.state,
      ) => {
    let newDataset: input = {
      "key": None,
      "basepath": values.basepath,
      "schedule": stringToOption(values.schedule),
      "packSize": packsizeToString(values.pack_size),
      "stores": stringToArray(values.stores),
    };
    let update = DefineDataset.make(~dataset=newDataset, ());
    // ignore the returned promise, the result will be delivered later
    mutate(
      ~variables=update##variables,
      ~refetchQueries=[|"getAllDatasets"|],
      (),
    )
    |> ignore;
  };
  [@react.component]
  let make = () => {
    <DefineDatasetMutation>
      ...{(mutate, {result}) => {
        let datasetForm =
          <DatasetFormRe
            initial={basepath: "", schedule: "", pack_size: "", stores: ""}
            onSubmit={submitNewDataset(mutate)}
          />;
        switch (result) {
        | Loading => <p> {ReasonReact.string("Preparing...")} </p>
        | Error(error) =>
          Js.log(error);
          <div> {ReasonReact.string(error##message)} </div>;
        | Data(_result) => datasetForm
        | NotCalled => datasetForm
        };
      }}
    </DefineDatasetMutation>;
  };
};

/*
 * Convert the BigInt value (a string) into a number, then convert from
 * bytes to megabytes, then convert back to a string.
 */
let stringToPacksize = (bigint: Js.Json.t): string =>
  switch (Js.Json.decodeString(bigint)) {
  | None => "INVALID BIGINT"
  | Some(str) =>
    let result =
      try (int_of_string(str)) {
      | Failure(_) => 0
      };
    let value = result / 1048576;
    string_of_int(value);
  };

module EditDatasetPanel = {
  let submitEditDataset =
      (
        mutate: UpdateDatasetMutation.apolloMutation,
        key: string,
        values: DatasetForm.state,
      ) => {
    let newDataset: input = {
      "key": Some(key),
      "basepath": values.basepath,
      "schedule": stringToOption(values.schedule),
      "packSize": packsizeToString(values.pack_size),
      "stores": stringToArray(values.stores),
    };
    let update = UpdateDataset.make(~dataset=newDataset, ());
    // ignore the returned promise, the result will be delivered later
    mutate(
      ~variables=update##variables,
      ~refetchQueries=[|"getAllDatasets"|],
      (),
    )
    |> ignore;
  };
  let computeInitial = (dataset: t) => {
    let initial: DatasetForm.state = {
      basepath: dataset##basepath,
      schedule: Belt.Option.getWithDefault(dataset##schedule, ""),
      pack_size: stringToPacksize(dataset##packSize),
      stores: Js.Array.joinWith(", ", dataset##stores),
    };
    initial;
  };
  [@react.component]
  let make = (~dataset: t) => {
    <UpdateDatasetMutation>
      ...{(mutate, {result}) =>
        switch (result) {
        | Loading => <p> {ReasonReact.string("Saving the dataset...")} </p>
        | Error(error) =>
          Js.log(error);
          <div> {ReasonReact.string(error##message)} </div>;
        | Data(result) =>
          let initial = computeInitial(result##updateDataset);
          <DatasetFormRe
            initial
            onSubmit={submitEditDataset(mutate, dataset##key)}
            datasetKey={Some(dataset##key)}
          />;
        | NotCalled =>
          let initial = computeInitial(dataset);
          <DatasetFormRe
            initial
            onSubmit={submitEditDataset(mutate, dataset##key)}
            datasetKey={Some(dataset##key)}
          />;
        }
      }
    </UpdateDatasetMutation>;
  };
};

module DeleteDatasetPanel = {
  let submitDeleteDataset =
      (mutate: DeleteDatasetMutation.apolloMutation, key: string) => {
    let update = DeleteDataset.make(~key, ());
    // ignore the returned promise, the result will be delivered later
    mutate(
      ~variables=update##variables,
      ~refetchQueries=[|"getAllDatasets"|],
      (),
    )
    |> ignore;
  };
  [@react.component]
  let make = (~dataset: t) => {
    <DeleteDatasetMutation>
      ...{(mutate, {result}) =>
        switch (result) {
        | Loading => <p> {ReasonReact.string("Deleting the dataset...")} </p>
        | Error(error) =>
          Js.log(error);
          <div> {ReasonReact.string(error##message)} </div>;
        | Data(_result) => React.null
        | NotCalled =>
          <form
            onSubmit={_ => submitDeleteDataset(mutate, dataset##key)}
            id={deleteFormName(dataset##key)}
          />
        }
      }
    </DeleteDatasetMutation>;
  };
};

module Component = {
  [@react.component]
  let make = () => {
    let buildEditPanels = (datasets: array(t)) =>
      Array.map(
        (dataset: t) =>
          <div
            key={
              dataset##key;
            }>
            <DeleteDatasetPanel dataset />
            <EditDatasetPanel dataset />
            <hr />
          </div>,
        datasets,
      );
    <GetDatasetsQuery>
      ...{({result}) =>
        switch (result) {
        | Loading =>
          <div> {ReasonReact.string("Loading the datasets...")} </div>
        | Error(error) =>
          Js.log(error);
          <div> {ReasonReact.string(error##message)} </div>;
        | Data(data) =>
          <div>
            {ReasonReact.array(buildEditPanels(data##datasets))}
            <p>
              {React.string("Use the form below to add a new dataset.")}
            </p>
            <NewDatasetPanel />
          </div>
        }
      }
    </GetDatasetsQuery>;
  };
};