import requests
import pandas as pd
from pathlib import Path
import base64
import logging
import time

def convert_epoch_columns(df, inplace=True):
    """
    Convert columns containing 'date' or 'timestamp' in their name from epoch to datetime.
    Handles invalid or extreme values safely.
    """
    if not inplace:
        df = df.copy()

    # Include both 'date' and 'timestamp' columns
    date_cols = [c for c in df.columns if 'date' in c.lower() or 'timestamp' in c.lower()]
    for c in date_cols:
        s = pd.to_numeric(df[c], errors='coerce')

        # Drop NaNs before determining the time unit
        valid = s.dropna()
        if valid.empty:
            continue

        # Determine likely unit (milliseconds if median >= 1e12)
        unit = 'ms' if valid.median() >= 1e12 else 's'

        # Clip extreme values that cause overflow in conversion
        min_valid = -2208988800 if unit == 's' else -2208988800000
        max_valid = 1e11 if unit == 's' else 1e14
        s = s.clip(lower=min_valid, upper=max_valid)

        # Convert safely
        try:
            df[c] = pd.to_datetime(s, unit=unit, utc=True, errors='coerce')
        except Exception as e:
            print(f"Skipping {c} due to conversion error: {e}")
            df[c] = pd.NaT

    return df


def build_basic_auth_headers(username: str, password: str):
    """Compute Basic auth header once."""
    token = base64.b64encode(f"{username}:{password}".encode()).decode()
    return {"Authorization": f"Basic {token}"}

def get_assignment_summary(assignment_id: str, headers: dict):
    url = f"https://data.edulastic.com/assignment-summary?assignment_id={assignment_id}"
    try:
        timeout_ = 90
        response = requests.get(url, headers=headers, timeout=timeout_)

        return response
    except requests.RequestException as e:
        print("request failed assignment_id:", assignment_id, "error:", e)
        return None



def get_assignment_summaries(df_assessments: list, username: str, password: str):
    """Loop over DataFrame assessment_ids using one precomputed header."""
    headers = build_basic_auth_headers(username, password)
    holding_list = []
    for aid in df_assessments:
        print(aid)  
        resp = get_assignment_summary(aid, headers)
        if resp.status_code == 200:
            try:
                response_data = pd.DataFrame(resp.json())
                holding_list.append(response_data)
            except Exception as e:
                logging.info(f'No data available for {aid}, status code: {resp.status_code if resp else "No Response"}')
        else:
            logging.info(f'Failed to retrieve assessment_id {aid}, status code: {resp.status_code if resp else "No Response"}')

    results = pd.concat(holding_list, ignore_index=True)
    results = convert_epoch_columns(results)
    logging.info(f'The number of unique assessments in the results frame is {results["assessment_group_id"].nunique()}')
    return results


def get_test_info(test_id: str, headers: dict):
    """
    Fetch details of a test from Edulastic Test Info API.
    Args:
        test_id (str): The unique identifier of the test.
        headers (dict): Authentication headers (e.g., from build_basic_auth_headers).
    Returns:
        requests.Response: The HTTP response object, or None if request fails.
    """
    url = f"https://data.edulastic.com/test-info?test_id={test_id}"
    try:
        response = requests.get(url, headers=headers, timeout=35)
        response.raise_for_status()
        return response
    except requests.RequestException as e:
        print(f"Request failed for test_id {test_id}: {e}")
        return None


