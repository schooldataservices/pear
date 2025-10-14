import requests
import pandas as pd
from pathlib import Path
import base64
import logging

def build_basic_auth_headers(username: str, password: str):
    """Compute Basic auth header once."""
    token = base64.b64encode(f"{username}:{password}".encode()).decode()
    return {"Authorization": f"Basic {token}"}

def get_assignment_summary(assignment_id: str, headers: dict):
    url = f"https://data.edulastic.com/assignment-summary?assignment_id={assignment_id}"
    try:
        response = requests.get(url, headers=headers, timeout=30)
        
        return response
    except requests.RequestException as e:
        print("request failed assignment_id:", assignment_id, "error:", e)
        return None

def get_assignment_summaries(df_assignments: pd.DataFrame, username: str, password: str):
    """Loop over DataFrame assignment_ids using one precomputed header."""
    headers = build_basic_auth_headers(username, password)
    holding_list = []
    for aid in df_assignments.assignment_id.tolist():
        print(aid)
        resp = get_assignment_summary(aid, headers)
        if resp.status_code == 200:
            try:
                response_data = pd.DataFrame(resp.json())
                holding_list.append(response_data)
            except Exception as e:
                logging.info(f'No data available for {aid}, status code: {resp.status_code if resp else "No Response"}')
        else:
            logging.info(f'Failed to retrieve assignment_id {aid}, status code: {resp.status_code if resp else "No Response"}')

    results = pd.concat(holding_list, ignore_index=True)
    logging.info(f'The number of unique assessments in the results frame is {results["assessment_group_id"].nunique()}')
    return results

# Existing helper unchanged
def read_in_text_file(file_path: str):
    path = Path(file_path)
    df_assignments = pd.read_csv(
        path,
        header=None,
        names=["assignment_id"],
        dtype=str
    )
    df_assignments = (
        df_assignments
        .assign(assignment_id=df_assignments.assignment_id.str.strip())
        .query("assignment_id != ''")
        .drop_duplicates()
        .reset_index(drop=True)
    )
    return df_assignments



def convert_epoch_columns(df, inplace=True):
    """
    Convert columns containing 'date' in their name from epoch to datetime.
    Handles invalid or extreme values safely.
    """
    if not inplace:
        df = df.copy()

    date_cols = [c for c in df.columns if 'date' in c.lower()]
    for c in date_cols:
        s = pd.to_numeric(df[c], errors='coerce')

        # Drop NaNs before determining the time unit
        valid = s.dropna()
        if valid.empty:
            continue

        # Determine likely unit (milliseconds if median >= 1e12)
        unit = 'ms' if valid.median() >= 1e12 else 's'

        # Clip extreme values that cause overflow in conversion
        # Epoch range for pandas: roughly 1677-09-21 to 2262-04-11
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


def get_updated_assignments(username: str, password: str, date: int):
    url = f"https://data.edulastic.com/assignment-list?date={date}"
    
    try:
        response = requests.get(url, auth=(username, password))
        print("Status code:", response.status_code)
        print("Response text:", response.text)

        if response.status_code == 200 and response.json() is not None:
            logging.info(f'Assignments retrieved successfully. Here is what has been updated within the last 24 hours: {response.json()}')
            return(pd.DataFrame(response.json(), columns=['assignment_id']))

        elif response.status_code == 401:
            print("⚠️ Unauthorized — check username, password, or permissions.")
            
    except requests.RequestException as e:
        print("Request failed:", e)
