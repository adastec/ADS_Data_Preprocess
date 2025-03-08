import sys, os
import rosbag
import pandas as pd
import pprint
import re
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from data_extraction.rostopic_handler import load_rostopic_map, generate_extraction_instructions

def load_rosbag(file_path):
    """
    Opens the rosbag file, prints its start and end times,
    and returns a tuple (bag, start_time, end_time).
    """
    try:
        bag = rosbag.Bag(file_path, 'r')
        start_time = pd.to_datetime(bag.get_start_time(), unit='s')
        end_time = pd.to_datetime(bag.get_end_time(), unit='s')
        print(f"Successfully opened {file_path}")
        print(f"Bag start time: {start_time}, Bag end time: {end_time}")
        duration = end_time - start_time
        duration_minutes = duration.total_seconds() // 60
        duration_seconds = duration.total_seconds() % 60
        print(f"Bag duration: {int(duration_minutes)} minutes and {int(duration_seconds)} seconds")
        return bag, start_time, end_time, duration
    except Exception as e:
        print(f"Error opening rosbag file {file_path}: {e}")
        return None, None, None

def get_field_value(msg, field_path):
    """
    Extracts a (potentially nested) field from msg given a dot-separated field_path.
    Supports indexing if the field spec has a bracket notation.
    For example: 'control.debug_values.data[11]' will:
         1. Get msg.control
         2. Get the debug_values attribute
         3. Get the data attribute and then its 12th element (index 11)
    Returns None if any attribute or index is missing.
    """
    fields = field_path.split('.')
    value = msg
    # Regular expression to match something like "data[11]"
    index_regex = re.compile(r"(\w+)\[(\d+)\]")
    for f in fields:
        match = index_regex.match(f)
        if match:
            # f has the form: attribute[index]
            attr = match.group(1)
            idx = int(match.group(2))
            if not hasattr(value, attr):
                return None
            value = getattr(value, attr)
            # Now, value should be indexable
            try:
                value = value[idx]
            except (IndexError, TypeError):
                return None
        else:
            if not hasattr(value, f):
                return None
            value = getattr(value, f)
    return value

def extract_topic_data(bag, topic, field_path=None):
    """
    Iterates over messages in the given topic and collects the data.
    Each entry is stored as a tuple (timestamp, extracted_value).
    If field_path is provided, extracts that field from the message;
    otherwise, saves the whole message.
    
    The timestamp is taken from the message stamp if available:
      - First, it checks for msg.header.stamp.
      - If not present, it checks for msg.stamp.
      - Otherwise, it falls back to the rosbag's timestamp.
    """
    data = []
    try:
        for topic_name, msg, t in bag.read_messages(topics=[topic]):
            # Use the message's own stamp (if available) rather than the bag timestamp.
            if hasattr(msg, 'header') and hasattr(msg.header, 'stamp'):
                msg_time = msg.header.stamp.to_sec()
            elif hasattr(msg, 'stamp'):
                try:
                    msg_time = msg.stamp.to_sec()
                except AttributeError:
                    # If stamp doesn't have to_sec(), try converting directly.
                    msg_time = float(msg.stamp)
            else:
                msg_time = t.to_sec()
            
            if field_path:
                extracted = get_field_value(msg, field_path)
            else:
                extracted = msg
            data.append((msg_time, extracted))
        print(f"Extracted {len(data)} entries from topic '{topic}'")
    except Exception as e:
        print(f"Error reading messages for topic {topic}: {e}")
    return data

def convert_to_dataframe(topic_data):
    """
    Converts a list of (timestamp, message) tuples into a Pandas DataFrame.
    Assumes the message is a numeric value.
    Renames the data column to "value".
    """
    import pandas as pd
    df = pd.DataFrame(topic_data, columns=["timestamp", "msg"])
    # Rename 'msg' column to 'value' for consistent usage.
    df = df.rename(columns={"msg": "value"})
    # Convert the timestamp (assumed to be in seconds) to a datetime index.
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
    df.set_index("timestamp", inplace=True)
    return df

if __name__ == "__main__":
    # Define the rosbag file path.
    file_path = "/workspace/rosbags/RCAS_Intervention_ID_15_2025_02_14_17_37_33.bag"  # Update with a real file name

    # Open the rosbag and use only the bag instance for further functions.
    bag, start_time, end_time, duration = load_rosbag(file_path)
    if bag is None:
        exit(1)

    # Load rostopic mapping from YAML.
    try:
        topic_map = load_rostopic_map("/workspace/ADS_Data_Extractor/config/rostopic_mapping.yaml")
    except Exception as e:
        print(f"Failed to load YAML mapping: {e}")
        bag.close()
        exit(1)

    # Generate extraction instructions and a data-rostopic map.
    data_rostopic_map, extraction_list = generate_extraction_instructions(bag, topic_map)
    print("\nData-Rostopic Map (logical_name: selected_topic):")
    pprint.pprint(data_rostopic_map)
    print("\nExtraction Instructions List:")
    pprint.pprint(extraction_list)

    # Build a mapping from logical_name to field_path (if a candidate provides a more specific extraction)
    logical_field_map = {}
    for instruction in extraction_list:
        field_path = None
        base = instruction["selected_topic"]
        # Look for a candidate that's more specific than the base topic.
        for cand in instruction["candidates"]:
            if cand != base and cand.startswith(base + '.'):
                # field_path is the remaining part after the base topic and dot.
                field_path = cand[len(base) + 1:]
                break
        logical_field_map[instruction["logical_name"]] = field_path

    # Extract data for each topic based on the generated mapping.
    extracted_data = {}
    for logical_name, topic in data_rostopic_map.items():
        if topic is not None:
            field_path = logical_field_map.get(logical_name)
            if field_path:
                print(f"\nProcessing logical name '{logical_name}' -> topic '{topic}' with field '{field_path}'")
            else:
                print(f"\nProcessing logical name '{logical_name}' -> topic '{topic}'")
            extracted_data[logical_name] = extract_topic_data(bag, topic, field_path)
        else:
            print(f"\nNo matching topic found for logical name '{logical_name}'")
            extracted_data[logical_name] = []
    bag.close()

    # Report the number of entries and optionally display a DataFrame head.
    output_folder = "/workspace/ADS_Data_Extractor/output_parquet"
    for logical_name, entries in extracted_data.items():
        print(f"\nLogical name '{logical_name}' has {len(entries)} entries.")
        if entries:
            df = convert_to_dataframe(entries)
            print(df.head())
            output_file = f"{output_folder}/{logical_name}.csv"
            df.to_csv(output_file, index=False)
            print(f"Saved DataFrame for '{logical_name}' to {output_file}")

