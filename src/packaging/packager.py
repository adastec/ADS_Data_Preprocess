import os
import pyarrow as pa
import pyarrow.parquet as pq

def package_data(synchronized_data):
    """
    Package the synchronized data.
    
    In this example, we assume that synchronized_data is a dictionary
    mapping topic names to Pandas DataFrames. If further transformation
    is needed (e.g., merging topics into one DataFrame), it can be done here.
    
    Currently, it simply returns the data as-is.
    """
    return synchronized_data

def save_to_storage(packaged_data, output_path):
    """
    Save the packaged data to storage as Parquet files.
    
    If packaged_data is a dictionary (each topic has its own DataFrame),
    this function will create a directory at output_path (if it does not exist)
    and write each DataFrame to a separate Parquet file.
    
    For example, if a topic is "/topic1", it will be saved as "topic1.parquet".
    """
    if not os.path.exists(output_path):
        os.makedirs(output_path, exist_ok=True)
    
    for topic, df in packaged_data.items():
        # Create a safe filename by stripping leading slashes and replacing any others with underscores.
        safe_topic = topic.strip("/").replace("/", "_")
        file_path = os.path.join(output_path, f"{safe_topic}.parquet")
        # Convert the DataFrame to an Apache Arrow Table and write it to a Parquet file.
        table = pa.Table.from_pandas(df)
        pq.write_table(table, file_path)
        print(f"Saved packaged data for topic '{topic}' at {file_path}")
