
import pandas as pd
import logging

def normalize_after_concat(results, df):
    logging.info(f'Initial results shape: {results.shape}, df shape: {df.shape}')
    temp = pd.concat([results, df]).reset_index(drop=True)
    logging.info(f'Concatenated temp shape: {temp.shape}')

    id_cols = [
        'school_sis_id', 'assessment_group_id', 'assessment_id',
        'classsection_sis_id', 'class_roster_source_id',
        'user_id', 'studentsisid'
    ]

    for c in id_cols:
        if c in temp.columns:
            # Convert NaN to empty string, then to string type
            temp[c] = temp[c].fillna("").astype(str).str.replace(r"\.0$", "", regex=True)

    temp = temp.fillna('')

    temp['total_points'] = pd.to_numeric(temp['total_points'], errors='coerce')
    temp['max_points'] = pd.to_numeric(temp['max_points'], errors='coerce')
    # temp = temp.apply(lambda col: col.str.strip() if col.dtype == 'object' else col)

    return temp


# def drop_duplicates_func(df):
#     # Ensure submitted_date is datetime
#     df["submitted_date"] = pd.to_datetime(df["submitted_date"], errors="coerce")

#     # Sort so that non-null submitted_dates come first and newest at the top
#     df = df.sort_values(
#         by=["submitted_date"], 
#         ascending=[False]
#     )

#     # Drop duplicates per group
#     df = (
#         df.groupby(
#             ["assessment_group_id", "classsection_sis_id", "user_id"], 
#             as_index=False
#         )
#         .first()  # keeps the top record in each group (most recent or NaN if all are NaN)
#     )
#     logging.info(f'After dropping duplicates, df shape: {df.shape}')
#     return(df)



def drop_duplicates_func(df):
    df["submitted_date"] = pd.to_datetime(df["submitted_date"], errors="coerce")

    # Sort newest first
    df_sorted = df.sort_values(by=["submitted_date"], ascending=False).copy()

    # Mark duplicates (keep first = newest per group)
    df_sorted["is_duplicate"] = df_sorted.duplicated(
        subset=["assessment_group_id", "classsection_sis_id", "user_id"], 
        keep="first"
    )

    dropped = df_sorted[df_sorted["is_duplicate"] == True]
    kept = df_sorted[df_sorted["is_duplicate"] == False]

    logging.info(f"Before: {len(df_sorted)}, After: {len(kept)}, Dropped: {len(dropped)}")
    logging.info(f"Dropped rows sample:\n{dropped.head(10).to_string(index=False)}")

    # Drop duplicates and return cleaned DataFrame
    return kept, dropped


def manual_check_dropped(kept, dropped, assessment_group_id, user_id):
    print("kept frame")
    display(kept.loc[(kept['assessment_id'] == assessment_group_id) & (kept['user_id'] == user_id)])

    print("dropped frame")
    display(dropped.loc[(dropped['assessment_id'] == assessment_group_id) & (dropped['user_id'] == user_id)])