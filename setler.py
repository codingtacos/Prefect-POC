import logging
import prefect
import json
from prefect import task, Flow, Parameter
from prefect.triggers import all_successful, all_failed
from prefect.tasks.aws.lambda_function import LambdaInvoke, LambdaList
from prefect.engine.result.base import Result
from prefect.utilities.logging import get_logger

@task(name="SampleSheetValidator", trigger=all_successful)
def execute_sample_sheet_validator(payload):
    logger = prefect.context.get("logger")


    # arn = "arn:aws:lambda:us-east-1:148609763264:function:rp-sample-sheet-validator-lambda:c36bbec267f105dc5ad6825b95b78157d50d6275"

    # logger.info("Invoking LambdaList")
    # lambdas = LambdaList(master_region="us-east-1").run()
    # logger.info(lambdas)

    stringified_payload = json.dumps(payload)
    logger.info('Invoking SampleSheetValidator Lambda with params {} and payload {}'.format(payload, stringified_payload))
    result = LambdaInvoke(function_name='rp-sample-sheet-validator-lambda').run(payload=stringified_payload)
    # result = LambdaInvoke(function_name="rp-sample-sheet-validator-lambda").run(payload=payload)
    logger.info('SampleSheetValidator Lambda Invoked')
    # logger.info(json.dumps(result))
    logger.info(result.get('StatusCode'))
    logger.info(result.get('FunctionError'))

    invoke_result = result.get('Payload').read().decode()
    logger.info(invoke_result)

    return json.loads(invoke_result)

@task(name="BCL2FASTQ", trigger=all_successful)
def execute_bcl_2_fastq(payload):
    # No built-in support for batch (obviously)
    # TODO: hit transformhub
    return payload

@task(name="InterOp", trigger=all_successful)
def execute_interop(payload):
    # No built-in support for batch (obviously)
    # TODO: hit transformhub
    return payload

@task(name='Extract Batch Parameters', trigger=all_successful)
def extract_batch_parameters(result1, result2):
    return { **result1, **result2 }

@task(name='Update Bioinf DB', trigger=all_successful)
def update_bioinf_db(payload):
    logger = prefect.context.get("logger")

    logger.info('Invoking Update Bioinf DB')

    # arn = "arn:aws:lambda:us-east-1:148609763264:function:rp-update-bioinfdb-lambda:20eca2453bc888e9a9a03e85bdb88c819bdd808c"
    response = LambdaInvoke(function_name="rp-update-bioinfdb-lambda").run(payload=json.dumps(payload))

    logger.info('StatusCode: {}'.format(response.get('StatusCode')))
    result = json.loads(response.get('Payload').read().decode())

    logger.info('ResponsePayload: {}'.format(result))
    return result

with Flow('Mini Setler') as flow:
    payload = dict({
        'experiment-id': Parameter('experiment-id'),
        'sample-bucket': Parameter('sample-bucket'),
        "bcl-bucket": Parameter("bcl-bucket"),
        "sns-arn-matcher": Parameter("sns-arn-matcher"),
        "dry-run": Parameter("dry-run"),
        "mock-bcl2fastq": Parameter("mock-bcl2fastq"),
        "log-level": Parameter("log-level"),
        "sample-sheet": Parameter("sample-sheet"),
        "sequencer": Parameter("sequencer"),
        "min-tar-bytes": Parameter("min-tar-bytes")
    })
    is_valid = execute_sample_sheet_validator(payload)

    bcl_result = execute_bcl_2_fastq(is_valid)
    interop_result = execute_interop(is_valid)
    result = update_bioinf_db(extract_batch_parameters(bcl_result, interop_result))
    Result(result)

flow.register(project_name='Test1')
