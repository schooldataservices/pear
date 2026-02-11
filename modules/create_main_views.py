import re
import pandas as pd
import logging

keywords = {
    "algebra ii": "Algebra II",
    "algebra i": "Algebra I",
    "geometry": "Geometry",
    "science": "Science",
    "math": "Math"
}

# Assessment ID overrides for curriculum and unit mapping
# These override the automatic categorization when assignment names are ambiguous
assessment_overrides = {
    "68b22e718bbeb32951baaddf": {"curriculum": "Chemistry", "unit": "Interim 1"},  # 10th Grade Science Interim #1
    "690bc60f9f7d11ef8ccd4ace": {"curriculum": "Algebra II", "unit": "Unit 3"},  # Grade 11 Math Unit 3 Assessment*
    "6917b5dd477a07bb7337dfe8": {"curriculum": "Geometry", "unit": "Unit 3"},  # Grade 10 Math Unit 3 Assessment
}

def categorize_curriculum(name: str, assessment_id: str = None) -> str:
    # Check assessment_id override first
    if assessment_id and assessment_id in assessment_overrides:
        return assessment_overrides[assessment_id]["curriculum"]
    
    name_lower = name.lower()
    for k, v in keywords.items():
        if k in name_lower:
            return v
    return "Other"


def extract_unit(name: str, assessment_id: str = None):
    # Check assessment_id override first
    if assessment_id and assessment_id in assessment_overrides:
        return assessment_overrides[assessment_id]["unit"]
    
    name_lower = name.lower()
    # Try to match "unit <number>"
    match_unit = re.search(r'unit\s*(\d+)', name_lower)
    if match_unit:
        return f"Unit {match_unit.group(1)}"
    # Try to match "interim #<number>" or "interim <number>"
    match_interim = re.search(r'interim\s*#?\s*(\d+)', name_lower)
    if match_interim:
        return f"Interim {match_interim.group(1)}"
    return None  # or "No Unit"

# Function to assign performance level
def get_performance_band(score):
    if score >= 90:
        return 4, "Exceeds Expectations"
    elif score >= 70:
        return 3, "Meets Expectations"
    elif score >= 60:
        return 2, "Approaching Expectations"
    else:
        return 1, "Does Not Meet Expectations"




def make_view_summaries(df, year, client):

    df['data_source'] = 'pear'
    df['year'] = year
    df['test_type'] = 'assessment'
    df['standard_code'] = 'percent'
    df['curriculum'] = df.apply(lambda row: categorize_curriculum(row['assignment_name'], row['assessment_id']), axis=1)
    df['unit'] = df.apply(lambda row: extract_unit(row['assignment_name'], row['assessment_id']), axis=1)

    # Apply the function
    df[['performance_band_level', 'performance_band_label']] = df['percent_score'].apply(
        lambda x: pd.Series(get_performance_band(x))
    )

    # Optional combined "Proficiency" column
    df['proficiency'] = df['performance_band_level'].astype(str) + " " + df['performance_band_label']

    df.rename(columns={'submitted_date': 'date_taken',
                    'studentsisid': 'local_student_id',
                    'assignment_name': 'title',
                    'percent_score': 'score'}, inplace=True)

    query = """
    SELECT student_number as local_student_id,
    grade_level as grade
    FROM `icef-437920.views.student_to_teacher`
    WHERE year = '25-26'
    """
    temp = client.query(query).to_dataframe()

    temp['local_student_id'] = temp['local_student_id'].astype(str)
    df['local_student_id'] = df['local_student_id'].astype(str)

    df = pd.merge(df, temp, on='local_student_id', how='left')

    df = df[['data_source', 'assessment_id', 'year', 'date_taken', 'grade', 'local_student_id', 'test_type', 'curriculum', 'unit', 'title', 'standard_code', 'score', 'performance_band_level', 'performance_band_label', 'proficiency']]

    return(df)


def make_view_assignments(df, year, client):
    df['data_source'] = 'pear'
    df['year'] = year
    df['test_type'] = 'assessment'
    df['curriculum'] = df.apply(lambda row: categorize_curriculum(row['assignment_name'], row['assessment_id']), axis=1)
    df['unit'] = df.apply(lambda row: extract_unit(row['assignment_name'], row['assessment_id']), axis=1)

        # Apply the function
    df[['performance_band_level', 'performance_band_label']] = df['percent_score'].apply(
        lambda x: pd.Series(get_performance_band(x))
    )

    # Optional combined "Proficiency" column
    df['proficiency'] = df['performance_band_level'].astype(str) + " " + df['performance_band_label']

    df = df.drop(columns=['score']) #drop old score column that is not a percent. Before changing percent_score name 

    df.rename(columns={'timestamp': 'date_taken',
                'student_sis_id': 'local_student_id',
                'assignment_name': 'title',
                'standard_notation': 'standard_code',
                'percent_score': 'score'}, inplace=True)
    
    query = """
    SELECT student_number as local_student_id,
    grade_level as grade
    FROM `icef-437920.views.student_to_teacher`
    WHERE year = '25-26'
    """
    temp = client.query(query).to_dataframe()

    temp['local_student_id'] = temp['local_student_id'].astype(str)
    df['local_student_id'] = df['local_student_id'].astype(str)
    
    # Log merge statistics
    logging.info(f"Before merge: {len(df)} rows, {df['local_student_id'].nunique()} unique local_student_ids")
    logging.info(f"Grade lookup table: {len(temp)} rows, {temp['local_student_id'].nunique()} unique local_student_ids")
    
    # Check for nulls before merge
    null_ids_before = df['local_student_id'].isna().sum()
    if null_ids_before > 0:
        logging.warning(f"{null_ids_before} rows have null local_student_id before merge")

    df = pd.merge(df, temp, on='local_student_id', how='left')
    
    # Log merge results
    matched_count = df['grade'].notna().sum()
    unmatched_count = df['grade'].isna().sum()
    logging.info(f"After merge: {matched_count} rows with grade, {unmatched_count} rows without grade (null)")

    df = df[['data_source', 'assessment_id', 'year', 'date_taken', 'grade', 'local_student_id', 'test_type', 'curriculum', 'unit', 'title', 'standard_code', 'score', 'performance_band_level', 'performance_band_label', 'proficiency']]
    return(df)