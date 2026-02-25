

import ast
import logging
import pandas as pd

def transform_assignment_responses(df, client):
    # Log raw PEAR data: how many rows have no student ID (before any pipeline logic)
    if 'student_sis_id' in df.columns:
        raw_null_student = df['student_sis_id'].isna().sum()
        raw_empty_student = (df['student_sis_id'].astype(str).str.strip() == '').sum()
        logging.info(f"[Assignment responses] Raw data from PEAR: {len(df)} rows, {raw_null_student} with null student_sis_id, {raw_empty_student} with empty student_sis_id")
    else:
        logging.warning("[Assignment responses] Raw PEAR data has no 'student_sis_id' column")

    # Track the three special assignments
    special_assignment_ids = ['68c0991821a3b97a63808f7a', '689bb78d965cf7826eb6444d', '68e5793913c3d26b49c17750']
    
    # Check what these assignments have before filtering
    if 'assessment_id' in df.columns:
        special_rows = df[df['assessment_id'].isin(special_assignment_ids)]
        if not special_rows.empty:
            logging.info(f"Special assignments before filtering: {len(special_rows)} rows")
            for aid in special_assignment_ids:
                aid_rows = special_rows[special_rows['assessment_id'] == aid]
                if not aid_rows.empty:
                    test_types = aid_rows['test_type'].unique() if 'test_type' in aid_rows.columns else []
                    grading_statuses = aid_rows['grading_status'].unique() if 'grading_status' in aid_rows.columns else []
                    logging.info(f"  Assignment {aid}: test_type={test_types}, grading_status={grading_statuses}, rows={len(aid_rows)}")
    
    temp = df[['test_type', 'assessment_id', 'assignment_name', 'standard_notation', 'student_sis_id', 'question_index', 'score', 'max_score', 'grading_status', 'timestamp']].copy()
    
    # Log counts before filtering
    initial_count = len(temp)
    logging.info(f"Total rows before filtering: {initial_count}")
    
    temp = temp.loc[(temp['grading_status'] == 'GRADED') & (temp['test_type'].isin(['school common assessment', 'common assessment']))]

    # Log counts after filtering
    filtered_count = len(temp)
    logging.info(f"Total rows after filtering (GRADED + common assessment): {filtered_count}")
    logging.info(f"Rows filtered out: {initial_count - filtered_count}")
    
    # Check if special assignments remain after filtering
    if 'assessment_id' in temp.columns:
        special_after = temp[temp['assessment_id'].isin(special_assignment_ids)]
        if special_after.empty:
            logging.warning(f"All special assignments were filtered out! None of the 3 special assignments remain after filtering.")
        else:
            logging.info(f"Special assignments after filtering: {len(special_after)} rows")

    # Handle standard_notation - some assignments may not have standards attached
    def parse_standard_notation(x):
        if pd.isna(x) or x == '' or x is None:
            return []
        if isinstance(x, str):
            try:
                return ast.literal_eval(x)
            except (ValueError, SyntaxError):
                logging.warning(f"Could not parse standard_notation: {x}")
                return []
        return x if isinstance(x, list) else []
    
    temp['standard_notation'] = temp['standard_notation'].apply(parse_standard_notation)
    
    # Filter out rows with empty standard_notation before explode
    rows_before_explode = len(temp)
    temp = temp[temp['standard_notation'].apply(lambda x: len(x) > 0 if isinstance(x, list) else False)]
    rows_after_explode_filter = len(temp)
    if rows_before_explode != rows_after_explode_filter:
        logging.info(f"Filtered out {rows_before_explode - rows_after_explode_filter} rows with empty standard_notation")
    
    temp = temp.explode('standard_notation').reset_index(drop=True)
    temp['percent_score'] = (temp['score'] / temp['max_score'] * 100).round(2)


    temp = swap_student_ids(temp, 'student_sis_id', client)

    return(temp)

    #Going to need to drop duplicates based on standard notation and student_sis_id somehow.



def swap_student_ids(df, mapping_column, client):
  # Make a copy to avoid SettingWithCopyWarning
  df = df.copy()

  # Count null/missing in source BEFORE any conversion (so we know "PEAR sent no ID" vs "ID not in PowerSchool")
  null_in_source = df[mapping_column].isna().sum()
  empty_in_source = (df[mapping_column].astype(str).str.strip().isin(['', 'nan'])).sum()
  logging.info(f"[{mapping_column}] Before mapping: {null_in_source} null in source data, {empty_in_source} empty/nan string in source")

  #Set to string to map values properly 
  df[mapping_column] = df[mapping_column].astype(str)
  
  # Log unique student IDs before mapping
  unique_before = df[mapping_column].nunique()
  sample_ids = df[mapping_column].unique()[:5] if len(df) > 0 else []
  logging.info(f"Mapping {unique_before} unique {mapping_column} values. Sample IDs: {sample_ids}")

  # Latest row per student across all partitions (so students not in max partition still map)
  query = """
  SELECT id, student_number
  FROM `icef-437920.powerschool.pq_StudentDemos`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY partitiontime DESC) = 1;
  """

  student_demos_df = client.query(query).to_dataframe()
  mapping_dict = dict(zip(student_demos_df['id'].astype(str), student_demos_df['student_number'].astype(str)))
  
  logging.info(f"Loaded {len(mapping_dict)} student ID mappings from PowerSchool")

  # Store original values for logging
  original_values = df[mapping_column].copy()

  df[mapping_column] = df[mapping_column].map(mapping_dict)
  
  # Log mapping results and null breakdown: from raw (no ID from PEAR) vs from unmapped (ID not in PowerSchool)
  mapped_count = df[mapping_column].notna().sum()
  total_null_after = df[mapping_column].isna().sum()
  unique_after = df[mapping_column].nunique()
  # After .map(): nulls are (1) original null/empty that became 'nan' and didn't match, or (2) ID not in mapping_dict
  null_from_source_approx = min(null_in_source + empty_in_source, total_null_after)
  null_from_unmapped_approx = total_null_after - null_from_source_approx
  logging.info(f"Mapping results: {mapped_count} mapped, {total_null_after} total null after mapping -> ~{null_from_source_approx} from missing/empty ID in raw data, ~{null_from_unmapped_approx} from ID not found in PowerSchool pq_StudentDemos")
  
  if total_null_after > 0:
    unmapped_original_ids = sorted(original_values[df[mapping_column].isna()].unique())
    logging.warning(f"All unmapped student IDs ({len(unmapped_original_ids)} unique): {unmapped_original_ids}")

  return(df)