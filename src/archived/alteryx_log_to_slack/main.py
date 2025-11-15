''' alteryx job status to slack '''
from datetime import datetime, time
import requests

CHANNEL = 'C05QFA18ZE1'
BOT_USER_OAUTH_TOKEN = 'x'
ALTERYX_TOKEN = 'x'
ALTERYX_API_KEY = 'x'

def read_last_run_time_from_file(last_run_time, file_path):
    ''' get run time from file '''
    try:
        with open(file_path, "r", encoding='utf-8') as f:
            last_run_time_str = f.read()
            if last_run_time_str.strip():  # Check if the file is empty
                last_run_time = datetime.fromisoformat(last_run_time_str)
            else:
                print("last_run_time.txt is empty. Using default value.")
    except FileNotFoundError:
        print("last_run_time.txt not found. Using default value.")
    except PermissionError:
        print("Permission denied when reading last_run_time.txt. Using default value.")
    except IOError:
        print("IOError occurred when reading last_run_time.txt. Using default value.")
    except Exception as error:
        print(f"Unexpected error: {error}. Using default value.")
    return last_run_time

def save_last_run_time_to_file(last_run_time, file_path):
    ''' save history '''
    try:
        with open(file_path, "w", encoding='utf-8') as f:
            f.write(last_run_time.isoformat())
    except PermissionError:
        print("Permission denied when writing to last_run_time.txt.")
    except IOError:
        print("IOError occurred when writing to last_run_time.txt.")
    except Exception as error:
        print(f"Unexpected error: {error}.")

def get_alteryx_job_status_since_last_run(last_run_time):
    ''' Replace with your Alteryx server details
     Step 1: Fetch Alteryx Job Status '''
    url = "https://your_alteryx_server/api/v1/jobs/"
    headers = {
        "Authorization": f"Bearer {ALTERYX_API_KEY}",
    }

    response = requests.get(url, headers=headers, timeout=10)
    recent_jobs = []

    if response.status_code == 200:
        all_jobs = response.json()

        for job in all_jobs:
            job_time = datetime.fromisoformat(job['completionTime'])
            if job_time > last_run_time:
                recent_jobs.append(job)

        return recent_jobs
    else:
        return None

def format_message(job_data):
    ''' format alteryx messages nicely '''
    message = "Alteryx Job Status:\n"
    for job in job_data:
        message += f"Job ID: {job['id']}, Status: {job['status']}\n"
    return message

def send_to_slack(message):
    '''Send to Slack'''
    url = "https://slack.com/api/chat.postMessage"
    headers = {
        "Authorization": f"Bearer {BOT_USER_OAUTH_TOKEN}"
    }
    payload = {
        "channel": CHANNEL,
        "text": message
    }
    requests.post(url, headers=headers, json=payload, timeout=10)

def main():
    ''' execute skynet '''
    today_date = datetime.now().date()
    start_of_today = datetime.combine(today_date, time())
    last_run_time = start_of_today

    # Check if there is a saved last_run_time, otherwise use start of today
    last_run_time = read_last_run_time_from_file(last_run_time, "last_run_time.txt")

    recent_jobs = get_alteryx_job_status_since_last_run(last_run_time)
    if recent_jobs:
        message = format_message(recent_jobs)
        send_to_slack(message)
    else:
        send_to_slack("No recent Alteryx jobs or failed to fetch job status.")

    # Update last_run_time and save to file
    last_run_time = datetime.now()
    save_last_run_time_to_file(last_run_time, "last_run_time.txt")

if __name__ == "__main__":
    main()
