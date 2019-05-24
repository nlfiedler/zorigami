# Design

## Diagrams

### Requirements

Install the [graphviz](https://www.graphviz.org) tool (using Homebrew) and then
invoke `dot` from the command-line to generate the images. For example:

```shell
$ dot -Tpng:quartz:quartz modules.dot > modules.png
```

### Module Dependency

The `modules.dot` diagram shows the dependencies between the Rust modules.
