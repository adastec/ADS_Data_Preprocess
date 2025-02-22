import rosbag
import pandas as pd

def load_rosbag(file_path):
    """
    Opens the rosbag file and returns a Bag object.
    """
    try:
        bag = rosbag.Bag(file_path, 'r')
        print(f"Successfully opened {file_path}")
        return bag
    except Exception as e:
        print(f"Error opening rosbag file {file_path}: {e}")
        return None

def extract_topic_data(bag, topic):
    """
    Iterates over messages in the given topic and collects the data.
    Each entry is stored as a tuple (timestamp, message).
    
    Customize this function as needed to parse and convert message fields.
    """
    data = []
    try:
        for topic_name, msg, t in bag.read_messages(topics=[topic]):
            # For demonstration, we're storing the timestamp and the raw message.
            # In practice, you would extract specific fields from msg.
            data.append((t.to_sec(), msg))
        print(f"Extracted {len(data)} entries from topic '{topic}'")
    except Exception as e:
        print(f"Error reading messages for topic {topic}: {e}")
    return data

def extract_topics(file_path, topics):
    """
    Given a rosbag file and a list of topics, extract data for each topic.
    Returns a dictionary where keys are topic names and values are lists of (timestamp, message) tuples.
    """
    bag = load_rosbag(file_path)
    if bag is None:
        return None

    extracted_data = {}
    for topic in topics:
        print(f"Processing topic: {topic}")
        extracted_data[topic] = extract_topic_data(bag, topic)
    
    bag.close()
    return extracted_data

def convert_to_dataframe(topic_data):
    """
    Optional: Convert extracted topic data into a Pandas DataFrame.
    Customize the conversion based on the message structure.
    """
    # Example for one topic. You may need to do this for each topic separately.
    # Here, we assume that the message has attributes that can be converted to a dict.
    df = pd.DataFrame(topic_data, columns=["timestamp", "msg"])
    return df

if __name__ == "__main__":
    # For testing, define a sample file path and topics.
    # Make sure the file path points to a valid rosbag in your /rosbags directory.
    file_path = "/workspace/rosbags/RCAS_Intervention_ID_15_2025_02_14_17_37_33.bag"  # update with a real file name from your directory
    topics = ["/tf", "/tf_static"]    # update these topics based on your requirements

    data = extract_topics(file_path, topics)
    if data is not None:
        for topic, entries in data.items():
            print(f"Topic '{topic}' has {len(entries)} entries.")
            # Optionally, convert the data to a DataFrame and display head:
            df = convert_to_dataframe(entries)
            print(df.head())
