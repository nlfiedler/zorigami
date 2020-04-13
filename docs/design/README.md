# Design

## Clean Architecture

The [Clean
Architecture](https://blog.cleancoder.com/uncle-bob/2012/08/13/the-clean-architecture.html)
is suitable for large and small applications, and is based on
[SOLID](https://en.wikipedia.org/wiki/SOLID) design principles. It is a
combination of the onion architecture and hexagonal architecture that separates
the "policy" from the "details" of an application.

### Three Layer Design

The **presentation** layer is the one that the user and/or outside world
interacts with. In this application it consists of the widgets that make up the
graphical interface. The widgets themselves interact with the domain layer via
the business logic components (BLoC design pattern) which in turn operate on the
entities via the use cases defined in the next layer.

The **domain** layer is where the application logic is defined, in terms of use
cases operating on entities. The use cases interact with repositories for
storing and retrieving entities. Where and how this is done is not part of
the "policy", but rather "detail" that is handled in the next layer.

The **data** layer is where the details are handled. It is here that the
repositories defined in the domain layer are implemented, typically using data
sources to interact with the systems sitting outside of the application (e.g.
database, disk files, remote services).

## Diagrams

### Requirements

Install the [graphviz](https://www.graphviz.org) tool (using Homebrew) and then
invoke `dot` from the command-line to generate the images. For example:

```shell
$ dot -Tpng:quartz:quartz modules.dot > modules.png
```

### Module Dependency

The `modules.dot` diagram shows the dependencies between the Rust modules.
