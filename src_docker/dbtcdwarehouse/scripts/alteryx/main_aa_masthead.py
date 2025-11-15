""" run dbt """
import subprocess
import re
import logging
import sys  # Import the sys module
from logger import logger

logger("dbt-aa-masthead")

# Initialize logging
logging.basicConfig(
    filename="dbt_run.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def clean_ansi_codes(string):
    """clean output"""
    ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
    return ansi_escape.sub("", string)


try:
    # Define the dbt command you want to run
    # dbt_command = ["dbt", "run", "--select", "+tag:adw"]
    dbt_command = [
        "dbt",
        "build",
        "--select",
        "+tag:adobe",
    ]  # build introduce run test before materialise downstream models

    # Execute the command
    process = subprocess.Popen(
        dbt_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    stdout_data = []
    stderr_data = []

    # Collect stdout in real-time
    with process.stdout:
        for line in iter(process.stdout.readline, b""):
            clean_line = clean_ansi_codes(line.decode("utf-8").strip())
            logging.info("STDOUT: %s", clean_line)  # Log to file in real-time
            stdout_data.append(clean_line)

    # Wait for the process to complete
    process.wait()

    # Check for success based on exit code
    if process.returncode == 0:
        logging.info("dbt run was successful.")
    else:
        logging.error("dbt run encountered an error.")
        raise RuntimeError("dbt run failed.")

except Exception as error:
    logging.exception("An unexpected error occurred. %s", error)
    sys.exit(1)  # Exit with status code 1 to signal that the script failed
