import requests
from .get_assignment_summaries import build_basic_auth_headers, convert_epoch_columns
import pandas as pd
import time
import logging

def get_assignment_responses(assignment_id: str, headers: dict, date: int = None):
    """
    Fetch student responses for each question for a specific assignment.
    """
    base_url = "https://data.edulastic.com/assignment-responses"
    params = {"assignment_id": assignment_id}
    if date is not None:
        params["date"] = date

    try:
        response = requests.get(base_url, headers=headers, params=params, timeout=35)
        response.raise_for_status()
        return response
    except requests.RequestException as e:
        logging.error(f"Request failed for assignment_id {assignment_id}: {e}")
        return None


def get_assignment_responses_call(username: str, password: str, a_id_list: list, delay_seconds: int = 2):
    headers = build_basic_auth_headers(username, password)
    all_dataframes = []

    for idx, assignment_id in enumerate(a_id_list, 1):
        response = get_assignment_responses(assignment_id, headers)
        if response and response.status_code == 200:
            try:
                data = response.json()
                if isinstance(data, dict):
                    # some APIs return nested dicts — handle that
                    df = pd.json_normalize(data)
                else:
                    df = pd.DataFrame(data)
                df["assignment_id"] = assignment_id
                all_dataframes.append(df)
                print(f"✅ Collected data for {assignment_id} ({idx}/{len(a_id_list)})")
            except Exception as e:
                print(f"⚠️ Failed to parse JSON for {assignment_id}: {e}")
        else:
            print(f"⚠️ No data returned for {assignment_id}")

        time.sleep(delay_seconds)

    if all_dataframes:
        final_df = pd.concat(all_dataframes, ignore_index=True)
        final_df = convert_epoch_columns(final_df)
        # final_df.to_csv("all_assignment_responses.csv", index=False)
        # print("✅ Saved all responses to all_assignment_responses.csv")
        return final_df
    else:
        print("⚠️ No data collected.")
        return None

