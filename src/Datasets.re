//
// Copyright (c) 2019 Nathan Fiedler
//

type timeRange = {
  .
  "startTime": int,
  "stopTime": int,
};

type frequency = [ | `HOURLY | `DAILY | `WEEKLY | `MONTHLY];
type weekOfMonth = [ | `FIRST | `SECOND | `THIRD | `FOURTH | `FIFTH];
type dayOfWeek = [ | `SUN | `MON | `TUE | `WED | `THU | `FRI | `SAT];

type schedule = {
  .
  "frequency": frequency,
  "timeRange": option(timeRange),
  "weekOfMonth": option(weekOfMonth),
  "dayOfWeek": option(dayOfWeek),
  "dayOfMonth": option(int),
};

// The expected shape of the datasets from GraphQL.
type t = {
  .
  "key": string,
  "computerId": string,
  "basepath": string,
  "schedules": Js.Array.t(schedule),
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
        schedules {
          frequency
          timeRange {
            startTime
            stopTime
          }
          weekOfMonth
          dayOfWeek
          dayOfMonth
        }
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
  "schedules": Js.Array.t(schedule),
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
        schedules {
          frequency
          timeRange {
            startTime
            stopTime
          }
          weekOfMonth
          dayOfWeek
          dayOfMonth
        }
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
        schedules {
          frequency
          timeRange {
            startTime
            stopTime
          }
          weekOfMonth
          dayOfWeek
          dayOfMonth
        }
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
    | Schedules
    | PackSize
    | Stores;

  type state = {
    basepath: string,
    schedules: string, // Js.Array.t(schedule)
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

  module SchedulesField = {
    let update = (state, value) => {...state, schedules: value};

    let validator = {
      field: Schedules,
      strategy: Strategy.OnFirstSuccessOrFirstBlur,
      dependents: None,
      validate: _state => Ok(Valid),
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
    SchedulesField.validator,
    PackSizeField.validator,
    StoresField.validator,
  ];
};

module DatasetFormHook = Formality.Make(DatasetForm);

// For now just honoring an array of 0 or 1 elements, and only the
// frequency, no time range or selected days.
let stringFromSchedule = (inputs: Js.Array.t(schedule)): string => {
  switch (inputs) {
  | [||] => "none"
  | [|sched|] =>
    switch (sched##frequency) {
    | `HOURLY => "hourly"
    | `DAILY => "daily"
    | `WEEKLY => "weekly"
    | `MONTHLY => "monthly"
    }
  | _ => "none"
  };
};

let hourlySchedule: schedule = {
  "frequency": `HOURLY,
  "timeRange": None,
  "weekOfMonth": None,
  "dayOfWeek": None,
  "dayOfMonth": None,
};

let dailySchedule: schedule = {
  "frequency": `DAILY,
  "timeRange": None,
  "weekOfMonth": None,
  "dayOfWeek": None,
  "dayOfMonth": None,
};

let weeklySchedule: schedule = {
  "frequency": `WEEKLY,
  "timeRange": None,
  "weekOfMonth": None,
  "dayOfWeek": None,
  "dayOfMonth": None,
};

let monthlySchedule: schedule = {
  "frequency": `MONTHLY,
  "timeRange": None,
  "weekOfMonth": None,
  "dayOfWeek": None,
  "dayOfMonth": None,
};

// For now just converting a simple frequency string (e.g. "daily") into
// a schedule that reflects that frequency. And only one element, or none
// at all if the value is "none".
let scheduleFromString = (value: string): Js.Array.t(schedule) => {
  switch (value) {
  | "none" => [||]
  | "hourly" => [|hourlySchedule|]
  | "daily" => [|dailySchedule|]
  | "weekly" => [|weeklySchedule|]
  | "monthly" => [|monthlySchedule|]
  | _ => [||]
  };
};

let makeSchedule = (inputValue: string, onChange) => {
  let makeRadio = (label: string, value: string) => {
    let checked = value == inputValue;
    <label className="radio">
      <input type_="radio" name="frequency" value onChange checked />
      {ReasonReact.string(label)}
    </label>;
  };
  <div className="field is-horizontal" key="schedules">
    <div className="field-label is-normal">
      <label htmlFor="schedules" className="label">
        {ReasonReact.string("Schedule")}
      </label>
    </div>
    <div className="field-body">
      <div className="field">
        <div className="control">
          {makeRadio(" Manual", "none")}
          {makeRadio(" Hourly", "hourly")}
          {makeRadio(" Daily", "daily")}
          {makeRadio(" Weekly", "weekly")}
          {makeRadio(" Monthly", "monthly")}
        </div>
      </div>
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
      readOnly: bool,
    ) => {
  let formIsValid = validateMsg == "";
  let validationTextDiv =
    <p className="help is-danger" key="the_message">
      {ReasonReact.string(validateMsg)}
    </p>;
  let inputElem =
    switch (inputType) {
    | "textarea" =>
      let inputClass = formIsValid ? "textarea" : "textarea is-danger";
      <textarea
        id=inputId
        className=inputClass
        name=inputId
        onBlur
        onChange
        placeholder=placeholderText
        value=inputValue
        readOnly
      />;
    | _ =>
      let inputClass = formIsValid ? "input" : "input is-danger";
      <input
        id=inputId
        className=inputClass
        type_=inputType
        name=inputId
        value=inputValue
        onBlur
        onChange
        placeholder=placeholderText
        readOnly
      />;
    };
  let inputField =
    <div className="control" key="the_control"> inputElem </div>;
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
    let isEditing = Belt.Option.isSome(datasetKey);
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
           isEditing,
         )}
        {makeSchedule(form.state.schedules, event =>
           form.change(
             Basepath,
             DatasetForm.SchedulesField.update(
               form.state,
               event->ReactEvent.Form.target##value,
             ),
           )
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
           false,
         )}
        {formInput(
           "Pack Store(s)",
           "stores",
           "textarea",
           form.state.stores,
           "store/local/xyz123, store/sftp/abc456",
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
           isEditing,
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
 * Split the string on commas, discarding empty strings.
 */
let stringToArray = (str: string): array(string) => {
  let parts = Js.String.split(",", str);
  let trimmed = Array.map(s => String.trim(s), parts);
  Js.Array.filter(e => String.length(e) > 0, trimmed);
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
      "schedules": [||],
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
            initial={
              basepath: "",
              schedules: "",
              pack_size: "",
              stores: "",
            }
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
      "schedules": scheduleFromString(values.schedules),
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
  let computeInitial = (dataset: t): DatasetForm.state => {
    {
      basepath: dataset##basepath,
      schedules: stringFromSchedule(dataset##schedules),
      pack_size: stringToPacksize(dataset##packSize),
      stores: Js.Array.joinWith(", ", dataset##stores),
    };
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