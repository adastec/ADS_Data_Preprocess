def validate_data(synchronized_data):
    """
    Validates the synchronized data.
    
    For each topic (each represented as a Pandas DataFrame), the function checks:
      - That there are no missing values.
      - That the timestamp index is monotonically increasing.
    
    Returns True if all topics pass validation; otherwise, prints error messages and returns False.
    """
    all_valid = True
    for topic, df in synchronized_data.items():
        if df.isnull().values.any():
            print(f"Validation error: Missing values found in topic {topic}")
            all_valid = False
        if not df.index.is_monotonic_increasing:
            print(f"Validation error: Timestamps are not in order for topic {topic}")
            all_valid = False
    return all_valid
