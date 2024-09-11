[![Actions Status](https://github.com/bytewax/bytewax-clickhouse/workflows/CI/badge.svg)](https://github.com/bytewax/bytewax-clickhouse/actions)
[![PyPI](https://img.shields.io/pypi/v/bytewax-clickhouse.svg?style=flat-square)](https://pypi.org/project/bytewax-clickhouse/)
[![Bytewax User Guide](https://img.shields.io/badge/user-guide-brightgreen?style=flat-square)](https://docs.bytewax.io/stable/guide/index.html)

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://user-images.githubusercontent.com/6073079/195393689-7334098b-a8cd-4aaa-8791-e4556c25713e.png" width="350">
  <source media="(prefers-color-scheme: light)" srcset="https://user-images.githubusercontent.com/6073079/194626697-425ade3d-3d72-4b4c-928e-47bad174a376.png" width="350">
  <img alt="Bytewax">
</picture>

## bytewax-clickhouse

bytewax-clickhouse is commercially licensed with publicly available source code. Please see the full details in [LICENSE](./LICENSE.md).

## Installation

```bash
pip install bytewax-clickhouse
```

## Usage

Before running any workload, you will need to start ClickHouse if you are not already running it.

```bash
docker compose up -d
```

ClickHouse Sink requires a PyArrow table, Schema and keys for order and partition.

The Sink is eventually consistent based on the keys.

Add the import

```python
from bytewax.clickhouse import operators as chop
```

Add A schema and order by string to your code.

```python
CH_SCHEMA = """
        metric String,
        value Float64,
        ts DateTime,
        """

ORDER_BY = "metric, ts"
```

define a pyarrow schema

```python
PA_SCHEMA = pa.schema(
    [
        ("metric", pa.string()),
        ("value", pa.float64()),
        ("ts", pa.timestamp("us")),  # microsecond
    ]
)
```

Use the ClickHouse Sink to write data to ClickHouse

```python
chop.output(
    "output_clickhouse",
    metrics,
    "metrics",
    "admin",
    "password",
    database="bytewax",
    port=8123,
    ch_schema=CH_SCHEMA,
    order_by=ORDER_BY,
    pa_schema=PA_SCHEMA,
    timeout=timedelta(seconds=1),
    max_size=10,
)
```

## Setting up the project

### Install `just`

We use [`just`](https://just.systems/man/en/) as a command runner for
actions / recipes related to developing Bytewax. Please follow [the
installation
instructions](https://github.com/casey/just?tab=readme-ov-file#installation).
There's probably a package for your OS already.

### Install `pyenv` and Python 3.12

I suggest using [`pyenv`](https://github.com/pyenv/pyenv)
to manage python versions.
[the installation instructions](https://github.com/pyenv/pyenv?tab=readme-ov-file#installation).

You can also use your OS's package manager to get access to different
Python versions.

Ensure that you have Python 3.12 installed and available as a "global
shim" so that it can be run anywhere. The following will make plain
`python` run your OS-wide interpreter, but will make 3.12 available
via `python3.12`.

```console
$ pyenv global system 3.12
```

### Install `uv`

We use [`uv`](https://github.com/astral-sh/uv) as a virtual
environment creator, package installer, and dependency pin-er. There
are [a few different ways to install
it](https://github.com/astral-sh/uv?tab=readme-ov-file#getting-started),
but I recommend installing it through either
[`brew`](https://brew.sh/) on macOS or
[`pipx`](https://pipx.pypa.io/stable/).

## Development

We have a `just` recipe that will:

1. Set up a venv in `venvs/dev/`.

2. Install all dependencies into it in a reproducible way.

Start by adding any dependencies that are needed into [pyproject.toml](pyproject.toml) or into
[requirements/dev.in](requirements/dev.in) if they are needed for development.

Next, generate the pinned set of dependencies with

```console
> just venv-compile-all
```

## Create and activate a virtual environment

Once you have compiled your dependencies, run the following:

```console
> just get-started
```

Activate your development environment and run the development task:

```console
> . venvs/dev/bin/activate
> just develop
```

## License

`bytewax-clickhouse` is commercially licensed with publicly available
source code. You are welcome to prototype using this module for free,
but any use on business data requires a paid license. See
https://modules.bytewax.io/ for a license. Please see the full details
in [LICENSE](./LICENSE.md).
