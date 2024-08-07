# Intro to Prefect

This project is meant as an introduction to data engineering through Prefect.


## Setup

```shell
python -m venv .venv
.\.venv\Scripts\Activate
pip install -e '.[dev]'
pre-commit install
docker-compose up -d
prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"
prefect server start
```

## Teardown

```shell
docker-compose down
```
