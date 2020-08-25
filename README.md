# Prefect POC

## Getting prefect started

Install the prefect cli tool

```
pip install prefect
```

Start the local server

```
prefect server start
```

Navigate to `http://localhost:8080` to see the UI


## Adding 'flows'

Before running any flows, because the server is running locally, we need to change prefect over to use localhost. Do this by running:

```
prefect backend server
```

Add a flow by running the file with the prefect flow

```
python3 calculator.py
```
