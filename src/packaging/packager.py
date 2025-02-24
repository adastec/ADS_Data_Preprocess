import os
import logging
import pyarrow as pa
import pyarrow.parquet as pq

# Configure logging.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def package_data(merged_df):
    """
    Package the merged DataFrame for final storage.
    
    This function is designed to accept a single merged DataFrame that contains 
    the unified timeline, 'Duration', 'epoch', and one column per topic.
    
    Any further transformation (if needed) can be implemented here.
    
    Returns:
        merged_df (pandas.DataFrame): The packaged data ready for storage.
    """
    # Further transformations or metadata additions can be done here.
    return merged_df

def save_to_storage(final_df, output_path, filename="merged_data.parquet"):
    """
    Save the final merged DataFrame to storage as a single Parquet file.
    
    This function creates the output directory if it does not exist and writes
    the final merged DataFrame to a Parquet file.
    
    Parameters:
        final_df (pandas.DataFrame): The merged DataFrame containing synchronized data.
        output_path (str): Directory where the final Parquet file will be stored.
        filename (str): The file name of the final Parquet file (default "merged_data.parquet").
    """
    if not os.path.exists(output_path):
        os.makedirs(output_path, exist_ok=True)
        logging.info(f"Created output directory: {output_path}")
    else:
        logging.info(f"Using existing output directory: {output_path}")
    
    file_path = os.path.join(output_path, filename)
    try:
        # Convert the merged DataFrame to an Apache Arrow Table.
        table = pa.Table.from_pandas(final_df)
        # Write the table to a Parquet file.
        pq.write_table(table, file_path)
        logging.info(f"Saved merged data into Parquet at: {file_path}")
    except Exception as e:
        logging.error(f"Failed to save merged data to Parquet: {e}")

# Example usage:
if __name__ == "__main__":
    import pandas as pd
    import numpy as np

    # For demonstration: create a sample merged DataFrame.
    # In practice, this DataFrame would be produced by the synchronizer/validator modules.
    bag_start = pd.to_datetime("2025-02-14 14:37:18")
    bag_end = pd.to_datetime("2025-02-14 14:37:28")
    unified_index = pd.date_range(start=bag_start, end=bag_end, freq="10ms")
    
    # Dummy columns: Duration, epoch, Topic_A, and Topic_B.
    base_df = pd.DataFrame(index=unified_index)
    base_df["Duration"] = (unified_index - bag_start).total_seconds()
    base_df["epoch"] = unified_index.astype(np.int64) / 1e9
    base_df["Topic_A"] = np.sin(np.linspace(0, 2*np.pi, len(unified_index)))
    base_df["Topic_B"] = np.cos(np.linspace(0, 2*np.pi, len(unified_index)))
    
    # Package and save the merged DataFrame.
    merged_df = package_data(base_df)
    output_directory = "/workspace/ADS_Data_Extractor/output_parquet"
    save_to_storage(merged_df, output_directory)
