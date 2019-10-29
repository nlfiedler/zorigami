//
// Copyright (c) 2019 Nathan Fiedler
//

type fstype = [ | `DIRECTORY | `ERROR | `FILE | `SYM_LINK];

type treeEntry = {
  .
  "name": string,
  "fstype": fstype,
  "modTime": Js.Json.t,
  "reference": string,
};

type tree = {. "entries": array(treeEntry)};

module GetTree = [%graphql
  {|
    query GetTree($digest: Checksum!) {
      tree(digest: $digest) {
        entries {
          name
          fstype
          modTime
          reference
        }
      }
    }
  |}
];

module GetTreeQuery = ReasonApollo.CreateQuery(GetTree);

type snapshot = {
  .
  "checksum": Js.Json.t,
  "parent": option(Js.Json.t),
  "startTime": Js.Json.t,
  "endTime": option(Js.Json.t),
  "fileCount": Js.Json.t,
  "tree": Js.Json.t,
};

// The expected shape of the datasets from GraphQL.
type t = {
  .
  "key": string,
  "computerId": string,
  "basepath": string,
  "schedule": option(string),
  "latestSnapshot": option(snapshot),
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
        latestSnapshot {
          checksum
          parent
          startTime
          endTime
          fileCount
          tree
        }
        packSize
        stores
      }
    }
  |}
];

module GetDatasetsQuery = ReasonApollo.CreateQuery(GetDatasets);

let formatDate = (datetime: Js.Json.t): string =>
  switch (Js.Json.decodeString(datetime)) {
  | None => "INVALID DATE"
  | Some(str) =>
    let d = Js.Date.fromFloat(float_of_string(str) *. 1000.0);
    Js.Date.toLocaleString(d);
  };

let formatDateOption = (datetime: option(Js.Json.t)): string =>
  switch (datetime) {
  | None => ""
  | Some(value) => formatDate(value)
  };

let formatBigInt = (bigint: Js.Json.t): string =>
  switch (Js.Json.decodeString(bigint)) {
  | None => "INVALID BIGINT"
  | Some(str) => str
  };

// Show the schedule, if any, otherwise "manual".
let displaySchedule = (schedule: option(string)): string => {
  switch (schedule) {
  | None => "(manual)"
  | Some(sched) => sched
  };
};

// Show the time the latest snapshot finished, if there is one,
// or the time that it started (and still running). Or none if
// not latest snapshot.
let displayLatest = (dataset: t): string => {
  switch (dataset##latestSnapshot) {
  | None => "(none yet)"
  | Some(snap) =>
    switch (snap##endTime) {
    | None => "(running)"
    | Some(endt) => formatDate(endt)
    }
  };
};

// let hasEndTime = (dataset: t): bool => {
//   switch (dataset##latestSnapshot) {
//   | None => false
//   | Some(snap) =>
//     switch (snap##endTime) {
//     | None => false
//     | Some(_endt) => true
//     }
//   };
// };

let formatType = (fstype: fstype): string => {
  switch (fstype) {
  | `DIRECTORY => "DIR"
  | `ERROR => "ERROR"
  | `FILE => "FILE"
  | `SYM_LINK => "LINK"
  };
};

// Trim the uninteresting prefix from the reference value.
let formatReference = (reference: string): string =>
  if (Js.String.startsWith("file-", reference)) {
    // file-sha256-54b96c41e653070fe5071f72c13818bf48dc7cfb8ba9f7160d4a423b9738bcde
    Js.String.substringToEnd(
      ~from=12,
      reference,
    );
  } else if (Js.String.startsWith("tree-", reference)) {
    // tree-sha1-72e186d5cf58cf0e2545b6ed254354e671e0a9f4
    Js.String.substringToEnd(
      ~from=10,
      reference,
    );
  } else {
    reference;
  };

let makeFileModal = (~dataset: t, ~entry: treeEntry, ~cancel) => {
  // trim the leading "file-" prefix from the reference
  let checksum = Js.String.substringToEnd(~from=5, entry##reference);
  // link directly to the file restore function
  let fileUrl = "/restore/" ++ dataset##key ++ "/" ++ checksum ++ "/" ++ entry##name;
  <div className="modal is-active">
    <div className="modal-background" />
    <div className="modal-card">
      <header className="modal-card-head">
        <p className="modal-card-title">
          {ReasonReact.string(entry##name)}
        </p>
        <button className="delete" onClick=cancel />
      </header>
      <section className="modal-card-body">
        <p> {ReasonReact.string(formatDate(entry##modTime))} </p>
        <p>
          <code>
            {ReasonReact.string(formatReference(entry##reference))}
          </code>
        </p>
      </section>
      <footer className="modal-card-foot">
        <a href=fileUrl className="button is-primary">
          {ReasonReact.string("Restore")}
        </a>
        <button className="button" onClick=cancel>
          {ReasonReact.string("Cancel")}
        </button>
      </footer>
    </div>
  </div>;
};

let makeTreeTable = (rows: array(ReasonReact.reactElement)) => {
  <table className="table is-hoverable is-fullwidth">
    <thead>
      <tr>
        <th> {ReasonReact.string("Name")} </th>
        <th> {ReasonReact.string("Type")} </th>
        <th> {ReasonReact.string("Reference")} </th>
        <th> {ReasonReact.string("Date Modified")} </th>
      </tr>
    </thead>
    <tbody> {ReasonReact.array(rows)} </tbody>
  </table>;
};

let makeTreeEntry = (~entry: treeEntry, ~onClick) => {
  <tr
    key={
      entry##name;
    }
    onClick>
    <td> {ReasonReact.string(entry##name)} </td>
    <td> {ReasonReact.string(formatType(entry##fstype))} </td>
    <td>
      <code> {ReasonReact.string(formatReference(entry##reference))} </code>
    </td>
    <td> {ReasonReact.string(formatDate(entry##modTime))} </td>
  </tr>;
};

let makeNavUpElem = onClick => {
  <tr key="nav_up" onClick>
    <td colSpan=4> {ReasonReact.string("Go Up")} </td>
  </tr>;
};

module Tree = {
  [@react.component]
  let make = (~dataset: t, ~digest: Js.Json.t) => {
    let (snapshot, setSnapshot) = React.useState(() => digest);
    let (history, setHistory) = React.useState(() => []);
    let (selection, setSelection) = React.useState(() => None);
    let makeOneRow = (entry: treeEntry) => {
      let onClick = _ =>
        if (entry##fstype == `DIRECTORY) {
          setHistory(h => [snapshot, ...h]);
          setSnapshot(_ =>
            Js.Json.string(
              Js.String.substringToEnd(~from=5, entry##reference),
            )
          );
        } else if (entry##fstype == `FILE) {
          setSelection(_ => Some(entry));
        };
      makeTreeEntry(~entry, ~onClick);
    };
    let makeRows = (entries: array(treeEntry)) => {
      let base =
        Array.map((entry: treeEntry) => makeOneRow(entry), entries);
      if (List.length(history) > 0) {
        let upper =
          makeNavUpElem(_ => {
            setHistory(h => List.tl(h));
            setSnapshot(_ => List.hd(history));
          });
        Array.append([|upper|], base);
      } else {
        base;
      };
    };
    let modal =
      switch (selection) {
      | Some(entry) =>
        makeFileModal(~dataset, ~entry, ~cancel=_ => setSelection(_ => None))
      | None => React.null
      };
    let query = GetTree.make(~digest=snapshot, ());
    <GetTreeQuery variables=query##variables>
      ...{({result}) =>
        switch (result) {
        | Loading => <div> {ReasonReact.string("Loading the tree...")} </div>
        | Error(error) =>
          Js.log(error);
          <div> {ReasonReact.string(error##message)} </div>;
        | Data(data) =>
          switch (data##tree) {
          | None => <p> {ReasonReact.string("empty tree")} </p>
          | Some(tree) =>
            <div> modal {makeTreeTable(makeRows(tree##entries))} </div>
          }
        }
      }
    </GetTreeQuery>;
  };
};

module Snapshot = {
  [@react.component]
  let make = (~dataset: t) => {
    switch (dataset##latestSnapshot) {
    | None => React.null
    | Some(snap) =>
      <div>
        <nav className="level">
          <div className="level-item has-text-centered">
            <div>
              <p className="heading"> {ReasonReact.string("Start Time")} </p>
              <p className="title is-4">
                {ReasonReact.string(formatDate(snap##startTime))}
              </p>
            </div>
          </div>
          <div className="level-item has-text-centered">
            <div>
              <p className="heading"> {ReasonReact.string("End Time")} </p>
              <p className="title is-4">
                {ReasonReact.string(formatDateOption(snap##endTime))}
              </p>
            </div>
          </div>
          <div className="level-item has-text-centered">
            <div>
              <p className="heading"> {ReasonReact.string("File Count")} </p>
              <p className="title is-4">
                {ReasonReact.string(formatBigInt(snap##fileCount))}
              </p>
            </div>
          </div>
        </nav>
        <Tree dataset digest={snap##tree} />
      </div>
    };
  };
};

module Datasets = {
  [@react.component]
  let make = (~datasets: array(t)) => {
    let (dataset, setDataset) = React.useState(() => None);
    let makeRow = (dataset: t) => {
      let rowId = dataset##computerId ++ dataset##basepath;
      <tr key=rowId onClick={_ => setDataset(_ => Some(dataset))}>
        <td> {ReasonReact.string(dataset##computerId)} </td>
        <td> {ReasonReact.string(dataset##basepath)} </td>
        <td> {ReasonReact.string(displaySchedule(dataset##schedule))} </td>
        <td> {ReasonReact.string(displayLatest(dataset))} </td>
      </tr>;
    };
    <div>
      <table className="table is-hoverable is-fullwidth">
        <thead>
          <tr>
            <th> {ReasonReact.string("Computer")} </th>
            <th> {ReasonReact.string("Basepath")} </th>
            <th> {ReasonReact.string("Schedule")} </th>
            <th> {ReasonReact.string("Latest")} </th>
          </tr>
        </thead>
        <tbody> {ReasonReact.array(Array.map(makeRow, datasets))} </tbody>
      </table>
      {switch (dataset) {
       | Some(d) => <Snapshot dataset=d />
       | None => React.null
       }}
    </div>;
  };
};

module Component = {
  [@react.component]
  let make = () => {
    <GetDatasetsQuery>
      ...{({result}) =>
        switch (result) {
        | Loading =>
          <div> {ReasonReact.string("Loading the datasets...")} </div>
        | Error(error) =>
          Js.log(error);
          <div> {ReasonReact.string(error##message)} </div>;
        | Data(data) =>
          if (Belt.Array.length(data##datasets) > 0) {
            <Datasets datasets=data##datasets />;
          } else {
            <div className="notification is-warning">
              <p>
                {ReasonReact.string("Start by using the ")}
                <a href="/datasets"> {ReasonReact.string("Datasets")} </a>
                {ReasonReact.string(" page to define datasets.")}
              </p>
            </div>;
          }
        }
      }
    </GetDatasetsQuery>;
  };
};