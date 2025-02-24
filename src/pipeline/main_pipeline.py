# File: src/pipeline/main_pipeline.py

import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.rosbag_importer.importer import find_rosbag_files, select_rosbag_file
from src.data_extraction.extractor import load_rosbag, extract_topic_data, convert_to_dataframe
from src.resampling.synchronizer import create_final_timeseries  # returns a merged DF
from src.validation.validator import validate_data
from src.packaging.packager import package_data, save_to_storage

def run_pipeline():
    # Step 1: Find and select a rosbag file.
    rosbag_directory = "/workspace/rosbags"
    print("Scanning for rosbag files in:", rosbag_directory)
    rosbag_files = find_rosbag_files(directory=rosbag_directory)
    
    if not rosbag_files:
        print("No rosbag files found in the directory. Exiting pipeline.")
        return

    selected_file = select_rosbag_file(rosbag_files)
    if not selected_file:
        print("No file was selected. Exiting pipeline.")
        return

    print("Selected rosbag file:", selected_file)
    
    # Step 2: Load the rosbag and extract its start/end times.
    bag, bag_start, bag_end, duration = load_rosbag(selected_file)
    if bag is None:
        print("Failed to load rosbag. Exiting pipeline.")
        return

    # Step 3: Extract data from the selected rosbag for each topic.
    topics = ["/tf", "/tf_static20"]  # Replace with your actual list of topics.
    print("Extracting topics:", topics)
    extracted_data = {}
    for topic in topics:
        # Extract raw topic data from the bag (optionally, specify a field_path if needed)
        raw_data = extract_topic_data(bag, topic, field_path=None)
        # Convert raw data (list of tuples) into a DataFrame with a "value" column.
        df_topic = convert_to_dataframe(raw_data)
        extracted_data[topic] = df_topic
    bag.close()  # Close the bag once extraction is complete.
    if not extracted_data:
        print("Extraction failed. Exiting pipeline.")
        return

    # Step 4: Resample and synchronize the extracted data.
    # The synchronizer uses bag_start, bag_end, and the dictionary of topic DataFrames to create a merged DF.
    print("Resampling and synchronizing data...")
    merged_df = create_final_timeseries(bag_start, bag_end, extracted_data, frequency="10ms")
    
    # Step 5: Validate the merged DataFrame.
    print("Validating data quality...")
    if not validate_data(merged_df):
        print("Data validation failed. Check logs and fix issues before proceeding.")
        return

    # Step 6: Package the data.
    print("Packaging data...")
    packaged_data = package_data(merged_df)
    
    # Step 7: Save the packaged data to storage.
    output_path = "/workspace/packaged_data"  # Directory for the Parquet file.
    save_to_storage(packaged_data, output_path, filename="merged_data.parquet")
    
    print("Pipeline executed successfully. Packaged data saved at:", os.path.join(output_path, "merged_data.parquet"))

if __name__ == "__main__":
    run_pipeline()
