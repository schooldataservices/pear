import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/sam/icef-437920.json"
from modules.calls import *
from modules.access_secrets import *
from modules.normalizing import *
import logging
import sys
import time
from gcp_utils_sds import buckets
import pandas as pd
pd.set_option('display.max_columns', None)  # Show all columns when printing DataFrames



logging.basicConfig(
    level=logging.INFO,  # Adjust as needed (e.g., DEBUG, WARNING)
    format="%(asctime)s - %(message)s",  # Log format
    datefmt="%d-%b-%y %H:%M:%S",  # Date format
    handlers=[
        logging.StreamHandler(sys.stdout)  # Direct logs to stdout
    ],
    force=True  # Ensures existing handlers are replaced
)

def main():

    username = "icefdata@icefps.org"
    password = access_secret_version(project_id='icef-437920', secret_id='pear_password')

    now_sec = int(time.time())
    start_24h_ago_sec = now_sec - 24*3600
    r = get_updated_assignments(username, password, start_24h_ago_sec)

    if len(r) == 0:
        logging.info("No new assignments found. Exiting gracefully.")
        sys.exit(0) 

    results = get_assignment_summaries(r, username, password)
    results = convert_epoch_columns(results)


    gcs_path = "gs://pearbucket-icefschools-1/pear_assessments_initial.csv"
    df = pd.read_csv(gcs_path)

    temp = normalize_after_concat(results, df)
    # kept,dropped = drop_duplicates_func(temp)

    buckets.send_to_gcs('pearbucket-icefschools-1', "", temp, "pear_daily_updates.csv")
    return(temp)

temp = main()