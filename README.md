# PEAR Processing Pipeline

This project ingests assignment data from the Edulastic API, normalizes and deduplicates it, and writes the results to Google Cloud Storage.

## Workflow Overview

1. **Authentication**  
   Uses a Google service account (set `GOOGLE_APPLICATION_CREDENTIALS`).

2. **Fetch Updates**  
   - Retrieves assignment IDs updated in the last 24 hours from Edulastic.
   - If no updates, exits gracefully.

3. **Assignment Summaries**  
   - Fetches detailed summaries for each updated assignment.

4. **Data Normalization**  
   - Converts epoch timestamps to UTC datetimes.
   - Loads baseline data from GCS.
   - Merges and normalizes new and existing data.

5. **Deduplication**  
   - (Optional) Keeps only the latest submission per student/assignment.

6. **Output**  
   - Writes the processed data to a GCS bucket as `pear_daily_updates.csv`.

## Usage

- Run the pipeline:
  ```bash
  python main.py
  ```

- To build and push the Docker image:
  ```bash
  docker build -t pear-processing:latest .
  docker tag pear-processing:latest gcr.io/icef-437920/pear-processing:latest
  docker push gcr.io/icef-437920/pear-processing:latest
  ```

## Requirements

- Python 3.12+
- Google Cloud credentials with access to Secret Manager and GCS
- See `requirements.txt` for Python dependencies
- API docs are here https://docs.google.com/document/d/1OyXG4q9zF-W2mYW7wuGscxVeYStBmO5nF82gT2dfM04/edit?tab=t.0