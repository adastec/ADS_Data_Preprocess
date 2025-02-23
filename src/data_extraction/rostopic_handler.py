#!/usr/bin/env python3
import os
import yaml
import rosbag
import pprint

def load_rostopic_map(yaml_path="ADS_Data_Extractor/config/rostopic_mapping.yaml"):
    """
    Loads the rostopic mapping from a YAML file.
    Returns the mapping dictionary under the key 'rostopic_map'.
    """
    with open(yaml_path, "r") as f:
        mapping = yaml.safe_load(f)
    if mapping is None:
        raise ValueError("YAML file is empty or invalid: " + yaml_path)
    return mapping.get("rostopic_map", {})

def get_bag_topics(bag):
    """
    Retrieves the list of topics available in the given rosbag.
    Returns a set of topic names.
    """
    info = bag.get_type_and_topic_info()
    topics = set(info.topics.keys())
    print("Available topics in bag:", topics)
    return topics

def candidate_matches(candidate, available_topic):
    """
    Returns True if the candidate topic (from the YAML mapping) matches the available topic.
    If the candidate contains additional fields (e.g. '.pose.position.x'), it checks if the base topic
    (everything before the first '.') matches the available topic.
    """
    # Exact match
    if candidate == available_topic:
        return True
    # If candidate has extra fields, compare base topic
    if '.' in candidate:
        base_candidate = candidate.split('.')[0]
        if base_candidate == available_topic:
            return True
    return False

def get_matching_topic(candidate_list, available_topics):
    """
    Given a list of candidate topics from the YAML mapping and the set of available topics from the bag,
    returns the first candidate that matches (either exactly or based on the base topic).
    """
    for candidate in candidate_list:
        for topic in available_topics:
            if candidate_matches(candidate, topic):
                return topic
    return None

def select_available_topics(bag, topic_map):
    """
    For each information key in the topic mapping, checks which candidate topics are present in the bag.
    Returns a dictionary mapping each logical category and key to the selected topic and associated metadata.
    """
    available = {}
    bag_topics = get_bag_topics(bag)
    for category, items in topic_map.items():
        available[category] = {}
        for info_key, info_details in items.items():
            candidate_topics = info_details.get("topics", [])
            selected_topic = get_matching_topic(candidate_topics, bag_topics)
            available[category][info_key] = {
                "selected_topic": selected_topic,
                "description": info_details.get("description", ""),
                "unit": info_details.get("unit", ""),
                "type": info_details.get("type", "")
            }
    return available

def generate_extraction_instructions(bag, topic_map):
    """
    Builds two outputs:
    1. data_rostopic_map: a flat mapping from a logical identifier (category.info_key)
       to the selected ros topic in the bag.
    2. extraction_list: A list of extraction instructions containing candidate topics uploaded from YAML,
       the selected topic, and the extraction parameters (description, unit, type).
    """
    available_topics_map = select_available_topics(bag, topic_map)
    extraction_list = []
    for category, items in topic_map.items():
        for info_key, info_details in items.items():
            candidates = info_details.get("topics", [])
            selected_topic = available_topics_map[category][info_key]["selected_topic"]
            extraction_list.append({
                "logical_name": f"{category}.{info_key}",
                "selected_topic": selected_topic,
                "candidates": candidates,
                "description": info_details.get("description", ""),
                "unit": info_details.get("unit", ""),
                "type": info_details.get("type", "")
            })
    # Create a flat mapping from logical name to selected topic
    data_rostopic_map = {instruction["logical_name"]: instruction["selected_topic"] for instruction in extraction_list}
    return data_rostopic_map, extraction_list

if __name__ == "__main__":
    # Example: open a rosbag file and load the mapping.
    bag_file = "/workspace/rosbags/RCAS_Intervention_ID_14_2025_02_14_17_36_46.bag"
    try:
        bag = rosbag.Bag(bag_file, 'r')
    except Exception as e:
        print(f"Failed to open bag file {bag_file}: {e}")
        exit(1)
    
    try:
        # Adjust the YAML file path if necessary.
        topic_map = load_rostopic_map("ADS_Data_Extractor/config/rostopic_mapping.yaml")
    except Exception as e:
        print(f"Failed to load YAML mapping: {e}")
        bag.close()
        exit(1)
    
    # Generate mapping of available topics based on YAML mapping
    available_topics_map = select_available_topics(bag, topic_map)
    print("Mapping of available topics based on YAML mapping:")
    pprint.pprint(available_topics_map)

    # Generate extraction instructions and data-rostopic map.
    data_rostopic_map, extraction_list = generate_extraction_instructions(bag, topic_map)
    bag.close()

    print("\nData-Rostopic Map (logical_name: selected_topic):")
    pprint.pprint(data_rostopic_map)

    print("\nExtraction Instructions List:")
    pprint.pprint(extraction_list)
