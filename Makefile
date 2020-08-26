install:
	pip install prefect
	prefect backend server

start-server:
	prefect server start

start-agent: secrets
	prefect agent start

secrets:
	ttt aws-refresh write tempus-bioinformatics-pipeline-development
	$(shell ./set-aws-credentials.sh)

provision-project:
	prefect create project "Test1" -d "Test project to be used with toy-workflows"

deploy: calculator setler

calculator:
	python3 calculator.py

execute-calc:
	prefect run flow --name Arithmetic --project Test1 --parameters-string "{\"expression\": \"90 - 2\"}"

setler:
	python3 setler.py

execute-setler:
	prefect run flow --name "Mini Setler" --project Test1
