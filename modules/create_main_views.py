import re
import pandas as pd

keywords = {
    "algebra i": "Algebra I",
    "algebra ii": "Algebra II",
    "geometry": "Geometry",
    "science": "Science",
    "math": "Math"
}

def categorize_curriculum(name: str) -> str:
    name_lower = name.lower()
    for k, v in keywords.items():
        if k in name_lower:
            return v
    return "Other"


def extract_unit(name: str):
    match = re.search(r'unit\s*(\d+)', name.lower())
    if match:
        return f"Unit {match.group(1)}"
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
    df['curriculum'] = df['assignment_name'].apply(categorize_curriculum)
    df['unit'] = df['assignment_name'].apply(extract_unit)

    # Apply the function
    df[['performance_band_level', 'performance_band_label']] = df['percent_score'].apply(
        lambda x: pd.Series(get_performance_band(x))
    )

    # Optional combined "Proficiency" column
    df['proficiency'] = df['performance_band_level'].astype(str) + " " + df['performance_band_label']

    df.rename(columns={'assignment_id': 'assessment_id', 
                    'submitted_date': 'date_taken',
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
    df['curriculum'] = df['assignment_name'].apply(categorize_curriculum)
    df['unit'] = df['assignment_name'].apply(extract_unit)

        # Apply the function
    df[['performance_band_level', 'performance_band_label']] = df['percent_score'].apply(
        lambda x: pd.Series(get_performance_band(x))
    )

    # Optional combined "Proficiency" column
    df['proficiency'] = df['performance_band_level'].astype(str) + " " + df['performance_band_label']

    df.rename(columns={'assignment_id': 'assessment_id', 
                'timestamp': 'date_taken',
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

    df = pd.merge(df, temp, on='local_student_id', how='left')

    df = df[['data_source', 'assessment_id', 'year', 'date_taken', 'grade', 'local_student_id', 'test_type', 'curriculum', 'unit', 'title', 'standard_code', 'score', 'performance_band_level', 'performance_band_label', 'proficiency']]
    return(df)