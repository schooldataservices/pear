import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/sam/icef-437920.json"
from modules.access_secrets import *
from modules.normalizing import *
from modules.epoch_compliance import *
from modules.get_assignment_responses import *
from modules.transforming_assignment_responses import *
from modules.get_assignment_summaries import *
from modules.transforming_assignment_summaries import *
from modules.create_main_views import *
import logging
import sys
import time
from gcp_utils_sds import buckets
import pandas as pd
from google.cloud import bigquery
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

def main(year):
    client = bigquery.Client(project='icef-437920')
    username = "icefdata@icefps.org"
    password = access_secret_version(project_id='icef-437920', secret_id='pear_password')
    df = collect_daily_assignments(username, password, delay_seconds=1)    #To get all asssignments going back to beginning of August 
    logging.info(f'Here is the number of assignments since the beginning of the year {len(df)}')

    assignment_id_list = df.assignment_id.to_list() + [
    '68c0991821a3b97a63808f7a',
    '689bb78d965cf7826eb6444d',
    '68e5793913c3d26b49c17750'
    ] #assignments that are missing from Pears assignment-list endpoint because they do not have standards attached.

    df_assignment_responses_raw = get_assignment_responses_call(username, password, assignment_id_list)
    df_ar_transformed = transform_assignment_responses(df_assignment_responses_raw, client)
    assignments_view = make_view_assignments(df_ar_transformed, year, client)
    buckets.send_to_gcs('pearbucket-icefschools-1', "", df_assignment_responses_raw, "pear_assignment_responses_raw.csv")
    buckets.send_to_gcs('pearbucket-icefschools-1', "", df_ar_transformed, "pear_assignment_responses.csv")
    buckets.send_to_gcs('pearbucket-icefschools-1', "", assignments_view, "pear_assignment_responses_view.csv")
    # ---------------------------------------------------

    df_assignment_summaries_raw = get_assignment_summaries(assignment_id_list, username, password)
    df_assignment_summaries_transformed = transform_assignment_summaries(df_assignment_summaries_raw, client)
    summaries_view = make_view_summaries(df_assignment_summaries_transformed, year, client)
    buckets.send_to_gcs('pearbucket-icefschools-1', "", df_assignment_summaries_transformed, "pear_assignment_summaries.csv")
    buckets.send_to_gcs('pearbucket-icefschools-1', "", df_assignment_summaries_raw, "pear_assignment_summaries_raw.csv")
    buckets.send_to_gcs('pearbucket-icefschools-1', "", summaries_view, "pear_assignment_summaries_view.csv")

main(year='25-26')