# Use SolidJS for the client

- Status: accepted
- Deciders: Nathan Fiedler
- Date: 2026-04-04

## Context

For several years the front-end was written using [Dart](https://dart.dev) and [Flutter](https://flutter.dev) and it worked well enough, despite not being suited for running in a browser. An attempt was made to use a fine-grained reactive framework named [Leptos](https://leptos.dev), which allows for writing the entire application in Rust. However, that experiment proved to be too burdensome. See ADR #15 in [tanuki](https://github.com/nlfiedler/tanuki) for details.

[SolidJS](https://www.solidjs.com) is the front-end web framework on which the design of Leptos is largely based. It makes use of signals, actions, resources, and memos to create a fine-grained reactive web framework in TypeScript. It offers many more features than Leptos does as of 2026, and JavaScript works perfectly with this paradigm.

With regard to the language, [TypeScript](https://www.typescriptlang.org) works well and is easy to set up with [Vite](https://vite.dev). SolidJS itself is written in TypeScript and hence it makes sense to use it for the front-end.

## Decision

Choose **SolidJS** and **TypeScript** for the client application.

## Consequences

Tanuki switched to SolidJS five months earlier and it has been splendid, no reason to think this will be any different.

## Links

- SolidJS [website](https://www.solidjs.com)
- TypeScript [website](https://www.typescriptlang.org)
