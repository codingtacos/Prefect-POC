install:
	pip install prefect
	prefect backend server

start:
	prefect server start

calculator:
	python3 calculator.py
