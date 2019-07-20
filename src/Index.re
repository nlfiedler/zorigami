//
// Copyright (c) 2019 Nathan Fiedler
//
module App = {
  type route =
    | HomeRoute
    | NotFoundRoute;
  type state = {nowShowing: route};
  type action =
    | Navigate(route);
  let reducer = (_state, action) =>
    switch (action) {
    | Navigate(page) => {nowShowing: page}
    };
  let urlToShownPage = (url: ReasonReact.Router.url) =>
    switch (url.path) {
    | [] => HomeRoute
    | _ => NotFoundRoute
    };
  [@react.component]
  let make = () => {
    let (state, dispatch) =
      React.useReducer(
        reducer,
        {
          nowShowing:
            // Need to take the given URL in order to return to where we were
            // before as the backend may redirect to a specific page. When that
            // happens our application is effectively reloading from scratch.
            urlToShownPage(ReasonReact.Router.dangerouslyGetInitialUrl()),
        },
      );
    React.useEffect0(() => {
      let token =
        ReasonReact.Router.watchUrl(url =>
          dispatch(Navigate(urlToShownPage(url)))
        );
      Some(() => ReasonReact.Router.unwatchUrl(token));
    });
    let content =
      switch (state.nowShowing) {
      | HomeRoute => <Home.Component />
      | NotFoundRoute => <NotFound.Component />
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