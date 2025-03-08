# File: src/pipeline/main_pipeline.py

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.rosbag_importer.importer import find_rosbag_files, select_rosbag_file
from src.data_extraction.extractor import load_rosbag, extract_topic_data, convert_to_dataframe, get_field_value
from src.data_extraction.rostopic_handler import load_rostopic_map, generate_extraction_instructions
from src.resampling.synchronizer import create_final_timeseries  # returns a merged DF
from src.validation.validator import validate_data
from src.packaging.packager import package_data, save_to_storage

def load_all_messages(selected_file):
    """
    Loads all messages from the rosbag into memory.
    Returns a list of tuples (topic_name, msg, t), bag_start, bag_end.
    """
    bag, bag_start, bag_end, duration = load_rosbag(selected_file)
    all_messages = list(bag.read_messages())
    bag.close()
    return all_messages, bag_start, bag_end

def extract_topic_from_messages(all_messages, topic, field):
    """
    Using the pre-loaded list of messages, filter those belonging
    to the specified 'topic' and extract the desired field.
    """
    data = []
    # Filter messages for the specified topic.
    filtered = [ (msg, t) for topic_name, msg, t in all_messages if topic_name == topic ]
    for msg, t in filtered:
        # Use the message's own stamp if available.
        if hasattr(msg, 'header') and hasattr(msg.header, 'stamp'):
            msg_time = msg.header.stamp.to_sec()
        elif hasattr(msg, 'stamp'):
            try:
                msg_time = msg.stamp.to_sec()
            except AttributeError:
                msg_time = float(msg.stamp)
        else:
            msg_time = t.to_sec()
        # Extract the desired field if provided.
        if field:
            extracted = get_field_value(msg, field)
        else:
            extracted = msg
        data.append((msg_time, extracted))
    return convert_to_dataframe(data)

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
    
    # Step 2: Load the bag once into memory to get bag_start, bag_end and all messages.
    all_messages, bag_start, bag_end = load_all_messages(selected_file)
    
    rostopic_mapping_file = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'rostopic_mapping.yaml')
    try:
        topic_map = load_rostopic_map(rostopic_mapping_file)
    except Exception as e:
        print(f"Error loading rostopic mapping from {rostopic_mapping_file}: {e}")
        return

    try:
        data_rostopic_map, extraction_list = generate_extraction_instructions(all_messages, topic_map)
    except Exception as e:
        print(f"Failed to generate extraction instructions: {e}")
        return

    print("Generated extraction instructions:")
    for instruction in extraction_list:
        print(instruction)

    # Step 3: Sequential extraction for each mapped topic using the extraction_list.
    extracted_data = {}
    for instruction in extraction_list:
        logical_name = instruction["logical_name"]
        topic = instruction["selected_topic"]
        field = instruction.get("field")  # e.g., "pose.pose.position.x"
        if topic is not None:
            print(f"Extracting topic '{topic}' (logical name '{logical_name}'), field: {field}")
            try:
                df_topic = extract_topic_from_messages(all_messages, topic, field)
                extracted_data[logical_name] = df_topic
                print(f"Extraction completed for '{logical_name}', got {len(df_topic)} samples.")
            except Exception as e:
                print(f"Extraction failed for '{logical_name}' with error: {e}")
        else:
            print(f"No matching topic found for logical name '{logical_name}'. Skipping extraction.")

    if not extracted_data:
        print("Extraction failed. Exiting pipeline.")
        return

    # Step 4: Resample and synchronize the extracted data.
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
    
    print("Pipeline executed successfully. Packaged data saved at:",
          os.path.join(output_path, "merged_data.parquet"))

if __name__ == "__main__":
    run_pipeline()
