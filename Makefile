install:
	pip install prefect
	prefect backend server

start-server:
	prefect server start

start-agent:
	prefect agent start

provision-project:
	prefect create project "Test1" -d "Test project to be used with toy-workflows"

deploy: calculator

calculator:
	python3 calculator.py

execute-calc:
	prefect run flow --name Arithmetic --project Test1 --parameters-string "{\"expression\": \"90 - 2\"}"
