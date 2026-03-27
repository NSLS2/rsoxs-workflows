import os
from prefect import flow, get_run_logger, task

# from data_validation import general_data_validation
from export import export
from dotenv import load_dotenv


def get_api_key_from_env():
    with open("/srv/container.secret", "r") as secrets:
        load_dotenv(stream=secrets)
    api_key = os.environ["TILED_API_KEY"]
    return api_key


@task
def log_completion():
    logger = get_run_logger()
    logger.info("Complete")


@flow
def end_of_run_workflow(stop_doc, api_key=None, dry_run=None):
    uid = stop_doc["run_start"]
    if not api_key:
        api_key = get_api_key_from_env()

    # general_data_validation(uid)
    export(uid, api_key=api_key, dry_run=dry_run)
    log_completion()
