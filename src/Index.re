//
// Copyright (c) 2019 Nathan Fiedler
//
module GetConfiguration = [%graphql
  {|
    query {
      configuration {
        hostname
        username
        computerId
      }
    }
  |}
];

module GetConfigurationQuery = ReasonApollo.CreateQuery(GetConfiguration);

module Configuration = {
  [@react.component]
  let make = () => {
    <GetConfigurationQuery>
      ...{({result}) =>
        switch (result) {
        | Loading => <div> {ReasonReact.string("Loading...")} </div>
        | Error(error) =>
          Js.log(error);
          <div> {ReasonReact.string(error##message)} </div>;
        | Data(data) =>
          <div>
            <ul>
              <li>
                {ReasonReact.string(
                   "Username: " ++ data##configuration##username,
                 )}
              </li>
              <li>
                {ReasonReact.string(
                   "Hostname: " ++ data##configuration##hostname,
                 )}
              </li>
              <li>
                {ReasonReact.string(
                   "Computer ID: " ++ data##configuration##computerId,
                 )}
              </li>
            </ul>
          </div>
        }
      }
    </GetConfigurationQuery>;
  };
};

ReactDOMRe.renderToElementWithId(
  <ReasonApollo.Provider client=Client.instance>
    <Configuration />
  </ReasonApollo.Provider>,
  "main",
);