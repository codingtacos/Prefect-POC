include .env

install:
	pip install prefect
	prefect backend server

start-server:
	prefect server start

start-agent: secrets
	# AWS_DEFAULT_REGION=us-east-1 AWS_SESSION_TOKEN=$(AWS_SESSION_TOKEN) prefect agent start -e "PREFECT__CONTEXT__SECRETS__AWS_CREDENTIALS={ACCESS_KEY: $(AWS_ACCESS_KEY_ID), SECRET_ACCESS_KEY: $(AWS_SECRET_ACCESS_KEY)}"
	AWS_DEFAULT_REGION=us-east-1 AWS_SESSION_TOKEN=$(AWS_SESSION_TOKEN) prefect agent start -e "AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID)" -e "AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY)"

secrets:
	ttt aws-refresh write tempus-bioinformatics-pipeline-development
	./set-aws-credentials.sh

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
	prefect run flow --name "Mini Setler" --project Test1 --parameters-string "{\"experiment-id\": \"191125_A00897_0038_AHGWHVDRXX\",  \"sample-bucket\": \"bioinf-fastq-staging\",  \"bcl-bucket\": \"sequence-data-transform-novaseq04\",  \"sns-arn-matcher\": \"arn:aws:sns:us-west-2:841786612905:setler\",  \"dry-run\": \"False\",  \"mock-bcl2fastq\": \"True\",  \"log-level\": \"DEBUG\",  \"sample-sheet\": \"20191125_RNA_HG_noN_umi.csv\",  \"sequencer\": \"HiSeq05\",  \"min-tar-bytes\": \"500\"}"
