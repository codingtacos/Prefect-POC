import logging
import prefect
import json
import random
from datetime import timedelta
import boto3
from botocore.config import Config

import prefect
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

    lambda_payload = json.dumps({
        "state-machine-execution-id": prefect.context.flow_name,
        "experiment-id": payload.get("experiment-id"),
        "sample-sheet": payload.get("sample-sheet"),
        "bcl-bucket": payload.get("bcl-bucket"),
        "dry-run": payload.get("dry-run"),
        "log-level": payload.get("log-level"),
        "sequencer": payload.get("sequencer"),
        "sample-bucket": payload.get("sample-bucket"),
        "min-tar-bytes": payload.get("min-tar-bytes"),
        "mock-bcl2fastq": payload.get("mock-bcl2fastq")
    })

    logger.info('Invoking SampleSheetValidator Lambda with input {} and payload {}'.format(payload, lambda_payload))


    # arn = "arn:aws:lambda:us-east-1:148609763264:function:rp-sample-sheet-validator-lambda:c36bbec267f105dc5ad6825b95b78157d50d6275"
    result = LambdaInvoke(function_name='rp-sample-sheet-validator-lambda').run(payload=lambda_payload)

    logger.info('SampleSheetValidator Lambda Invoked with StatusCode: {}'.format(result.get('StatusCode')))

    invoke_result = result.get('Payload').read().decode()

    logger.info('ResponsePayload: {}'.format(invoke_result))
    logger.info(invoke_result)

    return json.loads(invoke_result)

@task(
    name="Poll AWS Batch",
    max_retries=60, # try for 60 minutes
    retry_delay=timedelta(minutes=1)
)
def poll_for_status(execution1, execution2):
    logger = prefect.context.get('logger')
    config = Config(
        region_name='us-east-1'
    )
    batch_client = boto3.client('batch', config=config)
    logger.info('Fetching jobs for \n{} \n\n{}'.format(execution1, execution2))

    jobs = batch_client.describe_jobs(jobs=[
        execution1.get('jobId'),
        execution2.get('jobId')
    ])

    logger.info('Type: {}'.format(type(jobs)))
    logger.info('Got jobs: {}'.format(jobs))

    # Check each job's status to verify it is complete

    for job in jobs.get('jobs'):
        if (job.get('status') != 'SUCCEEDED' or job.get('status') != 'FAILED'):
            raise AssertionError

    return {
        exit_code: jobs[0].attempts[0].container.exitCode,
        env: jobs[0].attempts[0].container.environment
    }

# Mock BCL2FASTQ, passes payload through
@task(name="BCL2FASTQ", trigger=all_successful,  tags=["batch"])
def execute_bcl_2_fastq(payload):
    logger = prefect.context.get("logger")
    logger.info('Executing BCL2FASTQ...')
    config = Config(
        region_name='us-east-1'
    )
    batch_client = boto3.client('batch', config=config)
    execution = batch_client.submit_job(
        jobName='bcl2fastq',
        jobQueue='arn:aws:batch:us-east-1:148609763264:job-queue/rp-bcl2fastq',
        jobDefinition='arn:aws:batch:us-east-1:148609763264:job-definition/rp-bcl2fastq',
        parameters={},
        containerOverrides={
            "vcpus": 16,
            "environment": [
                {
                    "name": "BCL_BUCKET",
                    "value": payload['bcl_bucket']
                },
                {
                    "name": "DATA_PRODUCT_ROOT_URL",
                    "value": "https://data-products.stagingtempus.com/"
                },
                {
                    "name": "DRY_RUN",
                    "value": payload['dry_run']
                },
                {
                    "name": "ENV",
                    "value": "development"
                },
                {
                    "name": "EXPERIMENT_ID",
                    "value": payload['experiment_id']
                },
                {
                    "name": "LIMS_V2_URL",
                    "value": "https://lims-service.stagingtempus.com/"
                },
                {
                    "name": "LOG_LEVEL",
                    "value": payload['log_level']
                },
                {
                    "name": "MACHINE_USER_EMAIL",
                    "value": "machine-user+rp-orchestration-staging-20181022@tempus.com"
                },
                {
                    "name": "MIN_TAR_BYTES",
                    "value": payload['min_tar_bytes']
                },
                {
                    "name": "MOCK_BCL2FASTQ",
                    "value": payload['mock_bcl2fastq']
                },
                {
                    "name": "OKTA_URL",
                    "value": "https://tempus.oktapreview.com/oauth2/ausfjxf2zb85ZfrrS0h7"
                },
                {
                    "name": "PAGER_DUTY_ARN",
                    "value": "arn:aws:sns:us-east-1:148609763264:pagerduty-us-east-1"
                },
                {
                    "name": "POST_TO_SLACK",
                    "value": "True"
                },
                {
                    "name": "SAMPLE_SHEET",
                    "value": payload['sample_sheet']
                },
                {
                    "name": "SEQUENCER",
                    "value": payload['sequencer']
                },
                {
                    "name": "SETLER_ID",
                    "value": payload['setler_id']
                },
                {
                    "name": "SLACK_DRY_RUN_CHANNEL",
                    "value": "bio_setler_staging_dry_run"
                },
                {
                    "name": "SLACK_ERROR_CHANNEL",
                    "value": "bio_setler_staging"
                },
                {
                    "name": "SLACK_INFO_CHANNEL",
                    "value": "bio_setler_staging"
                },
                {
                    "name": "START_TIME",
                    "value": payload['start_time']
                },
                {
                    "name": "TRACE_ID",
                    "value": payload['trace_id']
                },
                {
                    "name": "MATCHER_SNS_TOPIC_ARN",
                    "value": "arn:aws:sns:us-west-2:621002544405:bioinf_matcher_intake_dev"
                },
                {
                    "name": "SCIENCE_ACCT_ROLE_ARN",
                    "value": "arn:aws:iam::621002544405:role/bioinf-pipeline-preprod-write-fastq-to-s3"
                }
            ]
        }
    )
    logger.info('Completed BCL2FASTQ with result: {}'.format(execution))

    return execution

# Mock InterOp, passes payload through
@task(name="InterOp", trigger=all_successful,  tags=["batch"])
def execute_interop(payload):
    logger = prefect.context.get("logger")
    logger.info('Executing InterOp...')

    config = Config(
        region_name='us-east-1'
    )
    batch_client = boto3.client('batch', config=config)

    execution = batch_client.submit_job(
        jobName='interop',
        jobQueue='arn:aws:batch:us-east-1:148609763264:job-queue/rp-interop',
        jobDefinition='arn:aws:batch:us-east-1:148609763264:job-definition/rp-interop',
        containerOverrides={
            "vcpus": 16,
            "environment": [
                {
                    "name": "EXPERIMENT_ID",
                    "value": payload['experiment_id']
                },
                {
                    "name": "BCL_BUCKET",
                    "value": payload['bcl_bucket']
                },
                {
                    "name": "SAMPLE_BUCKET",
                    "value": payload['sample_bucket']
                },
                {
                    "name": "DRY_RUN",
                    "value": payload['dry_run']
                },
                {
                    "name": "LOG_LEVEL",
                    "value": payload['log_level']
                },
                {
                    "name": "SETLER_ID",
                    "value": payload['setler_id']
                },
                {
                    "name": "SAMPLE_SHEET",
                    "value": payload['sample_sheet']
                },
                {
                    "name": "START_TIME",
                    "value": payload["start_time"]
                },
                {
                    "name": "TRACE_ID",
                    "value": payload["trace_id"]
                }
            ]
        }
    )

    logger.info('Completed InterOp with result: {}'.format(execution))

    return execution

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

    # sometimes_throw(logger, 0.1)

    params = json.dumps(payload)

    logger.info('Invoking Update Bioinf DB with params: {}'.format(params))

    # arn = "arn:aws:lambda:us-east-1:148609763264:function:rp-update-bioinfdb-lambda:20eca2453bc888e9a9a03e85bdb88c819bdd808c"
    response = LambdaInvoke(function_name="rp-update-bioinfdb-lambda", invocation_type='RequestResponse').run(payload=params)

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
    sample_sheet_result = execute_sample_sheet_validator(payload)

    bcl2fastq_execution = execute_bcl_2_fastq(sample_sheet_result)
    interop_execution = execute_interop(sample_sheet_result)

    # update_bioinf_db_payload = dict({
    #     'env': '',
    #     'bcl2fastq-status': bcl_result['ResponseMetadata']
    # })

    result = update_bioinf_db(poll_for_status(bcl2fastq_execution, interop_execution))

    s3result = set_results(result)
    get_results(s3result)


flow.register(project_name='Test1')
