# File: src/pipeline/main_pipeline.py

import os
from src.rosbag_importer.importer import find_rosbag_files, select_rosbag_file
from src.data_extraction.extractor import extract_topics
from src.resampling.resampler import synchronize_topics
from src.packaging.packager import package_data, save_to_storage
from src.validation.validator import validate_data

def run_pipeline():
    # Step 1: Find and select a rosbag file.
    # Change the directory if needed. Here we use an absolute path:
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

    # Step 2: Extract data from the selected rosbag.
    # Define which topics you want to extract.
    topics = ["/tf", "/tf_static20"]  # Replace these with your actual topic names.
    print("Extracting topics:", topics)
    extracted_data = extract_topics(selected_file, topics)
    if extracted_data is None:
        print("Extraction failed. Exiting pipeline.")
        return

    # Step 3: Resample and synchronize the extracted data.
    print("Resampling and synchronizing data...")
    synchronized_data = synchronize_topics(extracted_data, frequency="10L")
    
    # Step 4: Validate the synchronized data.
    print("Validating data quality...")
    is_valid = validate_data(synchronized_data)
    if not is_valid:
        print("Data validation failed. Check logs and fix issues before proceeding.")
        return

    # Step 5: Package the data.
    print("Packaging data...")
    packaged_data = package_data(synchronized_data)
    
    # Specify an output path (adjust as needed)
    output_path = "/workspace/packaged_data/output_data.parquet"
    save_to_storage(packaged_data, output_path)
    
    print("Pipeline executed successfully. Packaged data saved at:", output_path)

if __name__ == "__main__":
    run_pipeline()
