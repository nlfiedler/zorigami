type state = {menuActive: bool};
type action =
  | ToggleMenu;
[@react.component]
let make = () => {
  let (state, dispatch) =
    React.useReducer(
      (state, action) =>
        switch (action) {
        | ToggleMenu => {menuActive: !state.menuActive}
        },
      {menuActive: false},
    );
  let menuClassName =
    state.menuActive ? "navbar-menu is-active" : "navbar-menu";
  <nav id="navbar" className="navbar is-transparent" role="navigation">
    <div className="navbar-brand">
      <img src="/images/cloud-protect.png" width="48" height="48" />
      <a
        role="button"
        className="navbar-burger"
        target="navMenu"
        onClick={_ => dispatch(ToggleMenu)}>
        <span ariaHidden=true />
        <span ariaHidden=true />
        <span ariaHidden=true />
      </a>
    </div>
    <div className=menuClassName id="navMenu">
      <div className="navbar-end">
        {ReactDOMRe.createElement(
           "a",
           ~props=
             ReactDOMRe.objToDOMProps({
               "className": "navbar-item tooltip is-tooltip-bottom",
               "data-tooltip": "Home",
               "onClick": _ => ReasonReact.Router.push("/"),
             }),
           [|
             <span className="icon">
               <i className="fas fa-lg fa-home" />
             </span>,
           |],
         )}
        {ReactDOMRe.createElement(
           "a",
           ~props=
             ReactDOMRe.objToDOMProps({
               "className": "navbar-item tooltip is-tooltip-bottom",
               "data-tooltip": "Manage stores",
               "onClick": _ => ReasonReact.Router.push("/stores"),
             }),
           [|
             <span className="icon">
               <i className="fas fa-lg fa-warehouse" />
             </span>,
           |],
         )}
        {ReactDOMRe.createElement(
           "a",
           ~props=
             ReactDOMRe.objToDOMProps({
               "className": "navbar-item tooltip is-tooltip-bottom",
               "data-tooltip": "GraphQL console",
               "href": "/graphiql",
             }),
           [|
             <span className="icon">
               <i className="fas fa-lg fa-search-plus" />
             </span>,
           |],
         )}
      </div>
    </div>
  </nav>;
};