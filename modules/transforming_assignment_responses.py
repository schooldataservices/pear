

def transform_assignment_responses(df, client):
    temp = df[['test_type', 'assessment_id', 'assignment_name', 'standard_notation', 'student_sis_id', 'question_index', 'score', 'max_score', 'grading_status', 'timestamp']]
    temp = temp.loc[(temp['grading_status'] == 'GRADED') & (temp['test_type'].isin(['school common assessment', 'common assessment']))]

    import ast

    temp['standard_notation'] = temp['standard_notation'].apply(
        lambda x: ast.literal_eval(x) if isinstance(x, str) else x
    )
    temp = temp.explode('standard_notation').reset_index(drop=True)
    temp['percent_score'] = (temp['score'] / temp['max_score'] * 100).round(2)


    temp = swap_student_ids(temp, 'student_sis_id', client)

    return(temp)

    #Going to need to drop duplicates based on standard notation and student_sis_id somehow.



def swap_student_ids(df, mapping_column, client):

  #Set to string to map values properly 
  df[mapping_column] = df[mapping_column].astype(str)

  query = """
  WITH MaxPartition_Demos AS (
    SELECT MAX(partitiontime) AS max_partitiontime
    FROM `icef-437920.powerschool.pq_StudentDemos`
  )
  SELECT id, student_number 
  FROM `icef-437920.powerschool.pq_StudentDemos` 
  WHERE partitiontime = (SELECT max_partitiontime FROM MaxPartition_Demos);
  """

  student_demos_df = client.query(query).to_dataframe()
  mapping_dict = dict(zip(student_demos_df['id'], student_demos_df['student_number']))

  df[mapping_column] = df[mapping_column].map(mapping_dict)

  return(df)