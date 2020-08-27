import logging
import prefect
import json
import random
from datetime import timedelta

from prefect.engine.state import Retrying
from prefect.engine.results.local_result import LocalResult
from prefect import task, Flow, Parameter
from prefect.triggers import all_successful, all_failed
from prefect.tasks.aws.lambda_function import LambdaInvoke, LambdaList
from prefect.engine.result.base import Result
from prefect.engine.results.s3_result import S3Result
from prefect.utilities.logging import get_logger


# Example of a method that might throw when called
def sometimes_throw(logger, chance_to_throw):
    num = random.random()
    logger.info('Rolled: {} / Chance: {}'.format(num, chance_to_throw))
    if (num < chance_to_throw):
        raise AssertionError()
    else:
        return

# Send a notification
def send_notification():
    logger = get_logger(name="SendNotification")
    logger.info('Sending notification, beep boop')

# On state change, checks the state and handles it if something happens
def notify_on_retry(task, old_state, new_state):
    if isinstance(new_state, Retrying): # check if new state is "Retrying"
        send_notification()
    return new_state

@task(
    name="SampleSheetValidator",
    trigger=all_successful, # Only execute if all previous tasks were successful
    tags=["lambda"],
    max_retries=3,
    retry_delay=timedelta(seconds=15),
    state_handlers=[notify_on_retry] # On state change, execute these functions
)
def execute_sample_sheet_validator(payload):
    logger = prefect.context.get("logger")

    sometimes_throw(logger, 0.1) # an error CAN occur here

    stringified_payload = json.dumps(payload)
    logger.info('Invoking SampleSheetValidator Lambda with params {} and payload {}'.format(payload, stringified_payload))

    # arn = "arn:aws:lambda:us-east-1:148609763264:function:rp-sample-sheet-validator-lambda:c36bbec267f105dc5ad6825b95b78157d50d6275"
    result = LambdaInvoke(function_name='rp-sample-sheet-validator-lambda').run(payload=stringified_payload)

    logger.info('SampleSheetValidator Lambda Invoked with StatusCode: {}'.format(result.get('StatusCode')))

    invoke_result = result.get('Payload').read().decode()

    logger.info('ResponsePayload: {}'.format(invoke_result))
    logger.info(invoke_result)

    return json.loads(invoke_result)

# Mock BCL2FASTQ, passes payload through
@task(name="BCL2FASTQ", trigger=all_successful,  tags=["batch"])
def execute_bcl_2_fastq(payload):
    logger = prefect.context.get("logger")
    logger.info('Executing BCL2FASTQ...')
    # TODO: hit transformhub
    logger.info('Completed BCL2FASTQ!')
    return payload

# Mock InterOp, passes payload through
@task(name="InterOp", trigger=all_successful,  tags=["batch"])
def execute_interop(payload):
    logger = prefect.context.get("logger")
    logger.info('Executing InterOp...')
    # TODO: hit transformhub
    logger.info('Completed InterOp!')
    return payload

# Merge parameters together, TODO: maybe prefect has a built in task for this
@task(name='Extract Batch Parameters', trigger=all_successful, tags=["pass"])
def extract_batch_parameters(result1, result2):
    return { **result1, **result2 }


@task(
    name='Update Bioinf DB',
    trigger=all_successful,
    tags=["lambda"],
    max_retries=3,
    retry_delay=timedelta(seconds=15),
    state_handlers=[notify_on_retry] # On state change, execute these functions
)
def update_bioinf_db(payload):
    logger = prefect.context.get("logger")

    sometimes_throw(logger, 0.1)

    params = json.dumps(payload)

    logger.info('Invoking Update Bioinf DB with params: {}'.format(params))

    # arn = "arn:aws:lambda:us-east-1:148609763264:function:rp-update-bioinfdb-lambda:20eca2453bc888e9a9a03e85bdb88c819bdd808c"
    response = LambdaInvoke(function_name="rp-update-bioinfdb-lambda").run(payload=params)

    logger.info('Update Bioinf DB invoked with StatusCode: {}'.format(response.get('StatusCode')))
    result = json.loads(response.get('Payload').read().decode())

    logger.info('ResponsePayload: {}'.format(result))
    return result

@task(name="Upload Results")
def set_results(data):
    logger = prefect.context.get("logger")
    logger.info('Writing S3 data')
    s3result = S3Result('rp-orch-setler-test-data').write(data)
    logger.info('Successfully wrote S3 data')
    return s3result

@task(name="Get Results")
def get_results(result):
    logger = prefect.context.get("logger")
    logger.info('Getting results...')
    data = result.read(result.location)
    logger.info('Found results: {}'.format(data.value))


with Flow('Mini Setler') as flow:
    payload = dict({
        'experiment-id': Parameter('experiment-id'),
        'sample-bucket': Parameter('sample-bucket'),
        "bcl-bucket": Parameter("bcl-bucket"),
        "sns-arn-matcher": Parameter("sns-arn-matcher"),
        "dry-run": Parameter("dry-run", default="True", required=False),
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

    s3result = set_results(result)
    get_results(s3result)

    local_result = LocalResult('/Users/eric.dobroveanu/code/POC/prefect/results').write(result)




flow.register(project_name='Test1')
