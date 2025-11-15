"""
source freshness
"""
import subprocess
import re
import sys

from ....common.logging.logger_config import setup_logger


def clean_ansi_codes(string):
    """Clean output from ANSI codes."""
    ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
    return ansi_escape.sub("", string)


def parse_failure_messages(data):
    """Parse failure messages from dbt output."""
    fail_msgs = [line for line in data if "error" in line.lower()]
    return fail_msgs


def run_dbt_freshness():
    """Run dbt freshness."""

    dbt_command = ["dbt", "source", "freshness"]
    try:
        logger = setup_logger("dbt_freshness", "dbt_freshness.log")

        # Execute dbt command and capture output
        process = subprocess.run(
            dbt_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )

        stdout_lines = clean_ansi_codes(process.stdout).split("\n")
        stderr_lines = clean_ansi_codes(process.stderr).split("\n")

        # Log stdout and stderr
        for line in stdout_lines:
            logger.info("STDOUT: %s", line)
        for line in stderr_lines:
            logger.error("STDERR: %s", line)

        # Parse and handle failures
        failure_messages = parse_failure_messages(stdout_lines + stderr_lines)
        if failure_messages:
            error_message = (
                "DBT freshness check failed for the following reasons:\n"
                + "\n".join(failure_messages)
            )
            logger.error(error_message)
            print(
                error_message, file=sys.stderr
            )  # Propagate detailed error message up

        # raise error if dbt run fails finally:
        if process.returncode != 0:
            raise RuntimeError(
                f"DBT run failed with exit code {process.returncode}"
            )

    except Exception as error:  # pylint: disable=broad-except
        logger.exception("An unexpected error occurred: %s", error)
        sys.exit(1)  # Exit with status code 1 to signal script failure


if __name__ == "__main__":
    run_dbt_freshness()
