# Prefect POC

https://docs.prefect.io/orchestration/concepts/cli.html

I have wrapped many of the commands in the `Makefile` of this project.

## Getting prefect started

Install the prefect cli tool and set the backend to `local` development

```
make install
```

Start the local server

```
make start-server
```

Start the prefect agent

```
make start-agent
```

Navigate to `http://localhost:8080` to see the UI


## Adding 'flows'

Add a flow by running the file with the prefect flow definition

```
make calculator
```

## Executing a flow

Execute the pre-baked flow via:
```
make exec-calc
```
