import logging
import prefect
from prefect import task, Flow, Parameter
from prefect.tasks.aws.lambda_function import LambdaInvoke
from prefect.engine.result.base import Result
from prefect.utilities.logging import get_logger


execution_payload = {
  "experiment-id": "191125_A00897_0038_AHGWHVDRXX",
  "sample-bucket": "bioinf-fastq-staging",
  "bcl-bucket": "sequence-data-transform-novaseq04",
  "sns-arn-matcher": "arn:aws:sns:us-west-2:841786612905:setler",
  "dry-run": "False",
  "mock-bcl2fastq": "True",
  "log-level": "DEBUG",
  "sample-sheet": "20191125_RNA_HG_noN_umi.csv",
  "sequencer": "HiSeq05",
  "min-tar-bytes": "500"
}

@task(name="SampleSheetValidator")
def execute_sample_sheet_validator(payload):
    logger = prefect.context.get("logger")

    arn = "arn:aws:lambda:us-east-1:148609763264:function:rp-sample-sheet-validator-lambda:c36bbec267f105dc5ad6825b95b78157d50d6275"
    result = LambdaInvoke(
        function_name="rp-sample-sheet-validator-lambda",
    ).run(payload=payload)

    logger.info('Invoked Lambda')
    logger.info(result)
    return result

@task(name="BCL2FASTQ")
def execute_bcl_2_fastq(payload):
    # No built-in support for batch (obviously)
    # TODO: hit transformhub
    return true

@task(name="InterOp")
def execute_interop(payload):
    # No built-in support for batch (obviously)
    # TODO: hit transformhub
    return true

@task(name='Extract Batch Parameters')
def extract_batch_parameters(result1, result2):
    return { result1, result2 }

@task(name='Update Bioinf DB')
def update_bioinf_db(payload):
    arn = "arn:aws:lambda:us-east-1:148609763264:function:rp-update-bioinfdb-lambda:20eca2453bc888e9a9a03e85bdb88c819bdd808c"
    return LambdaInvoke(
        function_name="rp-update-bioinfdb-lambda",
        payload=payload
    )

with Flow('Mini Setler') as flow:
    is_valid = execute_sample_sheet_validator(execution_payload)

    bcl_result = execute_bcl_2_fastq(is_valid)
    interop_result = execute_interop(is_valid)
    result = update_bioinf_db(extract_batch_parameters(bcl_result, interop_result))
    Result(result)

flow.register(project_name='Test1')
