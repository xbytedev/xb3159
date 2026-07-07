import json
import logging
import os
import subprocess
import sys
import time

import boto3
import psutil

# ----------------------------
# Configuration
# ----------------------------

AWS_REGION = "us-east-1"

QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/873435541166/xb3159-pdp"

SCRIPT_MAPPING = {
    "1mg_pdp_web": "Onemg_PDP.py",
    "amazon_pdp_web": "Amazon_PDP.py",
    "amazon_usa_pdp_web": "Amazon_com_PDP.py",
    "healthmug_pdp_web": "healthmug_PDP.py",
    "myntra_pdp_web": "Myntra_PDP.py",
    "nykaa_pdp_web": "Nykaa_PDP.py",
}

MAX_RUNTIME = 60 * 60

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("sqs_receiver.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

sqs = boto3.client("sqs", region_name=AWS_REGION)


# ---------------------------------------------------
# Process checker
# ---------------------------------------------------

def is_script_running(script_path):
    script_name = os.path.basename(script_path).lower()
    script_path = os.path.abspath(script_path).lower()

    for proc in psutil.process_iter(
        ["pid", "cmdline", "create_time"]
    ):
        try:
            cmdline = " ".join(proc.info["cmdline"] or []).lower()

            if script_name in cmdline or script_path in cmdline:
                return proc

        except (
            psutil.NoSuchProcess,
            psutil.AccessDenied,
            psutil.ZombieProcess,
        ):
            pass

    return None


# ---------------------------------------------------
# Launch crawler
# ---------------------------------------------------

def launch_script(script_name, object_ids):
    
    if script_name not in SCRIPT_MAPPING:
        logger.error("Unknown script %s", script_name)
        return False

    python_file = SCRIPT_MAPPING[script_name]

    script_path = os.path.join(
        os.path.dirname(__file__),
        python_file
    )

    if not os.path.exists(script_path):
        logger.error("%s not found", script_path)
        return False

    proc = is_script_running(script_path)

    if proc:

        runtime = time.time() - proc.create_time()

        if runtime < MAX_RUNTIME:
            logger.info(
                "%s already running (PID=%s)",
                python_file,
                proc.pid
            )
            return True

        logger.info(
            "%s running too long. Restarting...",
            python_file
        )

        try:
            proc.kill()
        except Exception:
            pass

    subprocess.Popen(
        [
            sys.executable,
            script_path,
            "--object-ids",
            json.dumps(object_ids),
        ],
        cwd=os.path.dirname(__file__),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )

    logger.info("Started %s", python_file)

    return True


# ---------------------------------------------------
# Process one SQS message
# ---------------------------------------------------

def process_message(message):

    body = json.loads(message["Body"])

    logger.info("Received message %s", body)

    script_name = body["script_name"]
    object_ids = body["object_ids"]

    success = launch_script(script_name,object_ids,)

    if success:
        sqs.delete_message(
            QueueUrl=QUEUE_URL,
            ReceiptHandle=message["ReceiptHandle"],
        )

        logger.info("Deleted SQS message")

    else:
        logger.error("Keeping message in queue for retry")


# ---------------------------------------------------
# Main loop
# ---------------------------------------------------

def main():

    logger.info("Starting SQS Receiver...")

    while True:

        try:

            response = sqs.receive_message(
                QueueUrl=QUEUE_URL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20,
                VisibilityTimeout=300,
            )

            messages = response.get("Messages", [])

            if not messages:
                continue

            for message in messages:

                try:
                    process_message(message)

                except Exception as e:
                    logger.exception(e)

        except KeyboardInterrupt:
            break

        except Exception as e:
            logger.exception(e)
            time.sleep(5)


if __name__ == "__main__":
    main()