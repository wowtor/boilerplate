# PROJECT

## Prerequisites

* Python 3
* A postgres database
* Virtual environment (recommended)

## Setup

```sh
virtualenv -p python3 venv
source venv/bin/activate
pip install -r requirements.txt
```

Create a file named `local.yaml` with the following contents:

```yaml
postgres.credentials.password: SECRET_PASSWORD
datadir: PATH_TO_DATA
```

Review the file named `PROJECT.yaml` and if any of these values should
be overridden, update `local.yaml` accordingly.

If the database server is `localhost`, you may create a database instance
by:

```sh
./run.py --run create_database
```

## Run

Get a list of available operations by:

```sh
./run.py --list
```

To run all operations, run:

```sh
./run.py
```

To run one or more specific operations, run:

```sh
./run.py --run NAME
```

For more help, try:

```sh
./run.py -h
```
