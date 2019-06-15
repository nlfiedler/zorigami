//
// Copyright (c) 2019 Nathan Fiedler
//
[@react.component]
let make = (~name) => {
  let (count, setCount) = React.useState(() => 0);
  <div>
    <p>
      {React.string(name ++ " clicked " ++ string_of_int(count) ++ " times.")}
    </p>
    <button className="button is-primary" onClick={_ => setCount(_ => count + 1)}>
      {React.string("Click me")}
    </button>
  </div>;
};