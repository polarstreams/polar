# Contributing to PolarStreams

Glad to know that you're interested in contributing to PolarStreams.

## Questions, bugs and features

Whether it's a bug report, feature request or a question, you're welcome to [raise an
issue](https://github.com/polarstreams/polar/issues).

We aim to respond to your issues soonest. If you wish to receive a faster response, we recommend you always describe
your steps and provide information about your environment in the bug reports. And if you're proposing a new feature,
it'll help us to evaluate the priority if you explain why you need it.

The label ["good first issue"](https://github.com/polarstreams/polar/labels/good%20first%20issue) marks tasks that are
beginner-friendly.

## Client libraries

[PolarStreams Client in golang][go-client] is the reference implementation under Apache License 2.0 and it's currently the
only client library we have. PolarStreams Client libraries are just thin HTTP client wrappers, for example the reference
implementation is less than 1K lines of code.

If you would like to author a client library, feel free to ping us so we can link it from the repository and website.
If you would like to author an official client library under https://github.com/polarstreams, reach out to us and we can
create the repo. In any case, the original author(s) will be credited in the project description.

## Environment setup

You need go 1.19+ to build PolarStreams from source.

### Run unit tests

Executing `go test` command with no additional flags will run the unit tests. Unit tests should complete in few seconds.

```shell
go test -v ./...
```

You can run a single unit test by setting the `-args` flag and setting the focus flag for [Ginkgo][ginkgo], for example:

```shell
go test ./internal/interbroker -v -args -ginkgo.focus "should marshal"
```

### Run integration tests

Set the build tag `integration` to run the integration tests. You can expect integration tests to take less than 5
minutes to complete.

```shell
go test -v -tags=integration -p=1 ./internal/test/integration
```

Note that in macOS, you need to manually create the alias for the loopback addresses `127.0.0.2` to `127.0.0.6` in order
to run the integration tests. For example:

```shell
for i in {2..6}; do sudo ifconfig lo0 alias 127.0.0.$i up; done
```

Development in Windows has not been tested yet. We recommend using the Windows Subsystem for Linux (WSL) to build and
run PolarStreams in your Windows development environment.

[go-client]: https://github.com/polarstreams/go-client
[ginkgo]: https://onsi.github.io/ginkgo/


## Building the documentation

The documentation is built using [MKDocs](https://www.mkdocs.org/) and it stored in the `docs` directory

### Prerequisites

You need Python 3 installed on your machine, as well as _pipenv_:

```shell
# From the `docs` directory
pip3 install pipenv # if you don't have it yet
pipenv install
```

### Structure

The website configuration is in the `mkdocs.yml` file.
The content is in the `docs` module.

### Build

You can build the web site using:

```shell
pipenv run mkdocs build
```

The first time you may need to install the mkdocs plugins:

```shell
pipenv run pip install mkdocs-macros-plugin
pipenv run pip install fontawesome_markdown
pipenv run pip install mkdocs-build-plantuml-plugin
pipenv run pip install mkdocs-macros-plugin
```

The website is generated in the `site` directory.
You can also enable the _serve_ mode, which update the website while you update it:

```shell
pipenv run mkdocs serve
```

### Upgrade the dependencies

```shell
pipenv update
```
