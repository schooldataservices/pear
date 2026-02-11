import pandas as pd
from .transforming_assignment_responses import swap_student_ids

def transform_assignment_summaries(df, client):
    results = df[['assignment_name', 'assessment_id', 'total_points', 'max_points', 'submitted_date', 'studentsisid', 'status']].copy()

    results = results.loc[(results['status'] == 'GRADED')]
    results['percent_score'] = (results['total_points'] / results['max_points'] * 100).round(2)

    results = swap_student_ids(results, 'studentsisid', client)

    results = results[
    results['assignment_name'].str.contains(r'test|assessment|interim', case=False, na=False)
    & ~results['assignment_name'].str.contains(r'review', case=False, na=False)
    ]

    return(results)


# Assessments only
# Only want to pull rows where the assignment_name contains “test”, “assessment”, “interim”
# One outlier is “Grade 8 Unit 1 T” (ID: #265d40)
# Exclude any assignment that contain “Review” in the title

#Link this up with pear_assignment_responses_raw.csv or  df_assignment_responses_raw to pull in test_type column 
#Merge on the assessment_id and bring in test_type