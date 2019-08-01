//
// Copyright (c) 2019 Nathan Fiedler
//
module App = {
  [@react.component]
  let make = () => {
    let url = ReasonReactRouter.useUrl();
    let content =
      switch (url.path) {
      | ["stores"] => <Stores.Component />
      | [] => <Home.Component />
      | _ => <NotFound.Component />
      };
    <div className="container">
      <Navbar />
      <main role="main"> content </main>
    </div>;
  };
};

ReactDOMRe.renderToElementWithId(
  <ReasonApollo.Provider client=Client.instance>
    <App />
  </ReasonApollo.Provider>,
  "main",
);