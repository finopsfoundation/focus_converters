## Prerequisites

- Python 3.8 or later
- Poetry

## Installation

### Step 1: Install Poetry

Poetry is a tool for dependency management and packaging in Python. To install Poetry, open your terminal and run the
following command:

#### Linux and MacOS

```sh
curl -sSL https://install.python-poetry.org | python3 -
```

#### Windows

Install python from [Microsoft store](https://apps.microsoft.com/store/detail/python-311/9NRWMJP3717K).

* Note: Installing from python.org also requires addition `certifi` package for SSL certs.

Install poetry

```shell
(Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | py -
```

From Powershell to allow poetry shell

```shell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned
```

* Note poetry would have to be added to PATH so that it can be used in steps below.
* Also running commands through poetry take care off env params,
  e.g. ```poetry run python -m focus_converter.main --help```

Refer to the [official Poetry documentation](https://python-poetry.org/docs/) for more installation options and details.

### Step 2: Clone the Project Repository

Clone the project repository to your local machine using the following command:

```sh
git clone https://github.com/finopsfoundation/focus_converters.git
```

### Step 3: Navigate to the Project Directory

Change your current directory to the project's directory:

```sh
cd focus_converter_base
```

### Step 4: Install Project Dependencies

Install the project's dependencies using Poetry:

```sh
poetry install --only main --no-root
```

or for installing all `dev` dependencies

```sh
poetry install --no-root
```

## Activating the Poetry Environment

```sh
poetry shell
```
