//
// Copyright (c) 2019 Nathan Fiedler
//
module Component = {
  [@react.component]
  let make = () => {
    <div>
      <div className="box"> <Config.Component /> </div>
      <Stores.GetStoresQuery>
        ...{({result}) =>
          switch (result) {
          | Loading =>
            <div> {ReasonReact.string("Counting the stores...")} </div>
          | Error(error) =>
            Js.log(error);
            <div> {ReasonReact.string(error##message)} </div>;
          | Data(data) =>
            if (Belt.Array.length(data##stores) > 0) {
              ReasonReact.null;
            } else {
              <div className="notification is-warning">
                <p>
                  {ReasonReact.string("Start by using the ")}
                  <a href="/stores"> {ReasonReact.string("Stores")} </a>
                  {ReasonReact.string(" page to define stores.")}
                </p>
              </div>;
            }
          }
        }
      </Stores.GetStoresQuery>
    </div>;
  };
};