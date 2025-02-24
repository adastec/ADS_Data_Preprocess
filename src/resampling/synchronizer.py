import pandas as pd
import numpy as np

def create_final_timeseries(bag_start, bag_end, synchronized_topics, frequency="10ms"):
    """
    Creates a unified time-series DataFrame with:
       - Index: timestamp at fixed frequency from bag_start to bag_end.
       - 'Duration': relative time in seconds (starting at 0).
       - 'epoch': epoch time (seconds since 1970-01-01).
       - One column per topic (logical name). Values will be NaN if a topic has not started.
    
    Parameters:
       bag_start: pd.Timestamp, rosbag start time.
       bag_end: pd.Timestamp, rosbag end time.
       synchronized_topics: dict of {logical_name: DataFrame}, where each DataFrame has a datetime index
                            and a 'value' column.
       frequency: str, the frequency for the unified timeline (default "10ms" for 100Hz).
    
    Returns:
       final_df: Merged DataFrame with the unified timeline.
    """
    # Create the unified timeline.
    unified_index = pd.date_range(start=bag_start, end=bag_end, freq=frequency)
    base_df = pd.DataFrame(index=unified_index)
    
    # Build the Duration and epoch columns.
    base_df["Duration"] = (unified_index - bag_start).total_seconds()
    base_df["epoch"] = unified_index.astype(np.int64) / 1e9

    # Reindex and rename each topic's DataFrame, then store for concatenation.
    topic_dfs = {}
    # Reindex and rename each topic's DataFrame, then store for concatenation.
    topic_dfs = {}
    for logical_name, df_topic in synchronized_topics.items():
        if "value" not in df_topic.columns:
            print(f"Warning: Topic '{logical_name}' data missing 'value' column.")
            continue
        # Reindex using 'nearest' method with a tolerance of 5ms.
        tolerance = pd.Timedelta("5ms")
        df_topic_reindexed = df_topic.reindex(unified_index, method='nearest', tolerance=tolerance)
        # Rename the column to the logical name.
        df_topic_reindexed = df_topic_reindexed.rename(columns={"value": logical_name})
        topic_dfs[logical_name] = df_topic_reindexed[logical_name]
        
    # Concatenate all topics with the unified base DataFrame.
    final_df = pd.concat([base_df] + list(topic_dfs.values()), axis=1)
    
    # Ensure Duration and epoch are the first columns.
    cols = ["Duration", "epoch"] + [col for col in final_df.columns if col not in ("Duration", "epoch")]
    final_df = final_df[cols]
    return final_df

if __name__ == "__main__":
    # For demonstration purposes, simulate a unified timeline and two topics.
    import time, random
    bag_start = pd.to_datetime("2025-02-14 14:37:18.267984128")
    bag_end = pd.to_datetime("2025-02-14 14:37:28.267984128")  # e.g., 10 seconds duration
    frequency = "10ms"  # 100 Hz

    # Create a unified index.
    unified_index = pd.date_range(start=bag_start, end=bag_end, freq=frequency)
    
    # Simulate topic A starting at bag_start.
    topic_a_data = np.sin(np.linspace(0, 2*np.pi, len(unified_index)))
    df_topic_a = pd.DataFrame({"value": topic_a_data}, index=unified_index)
    
    # Simulate topic B starting 1 second later.
    offset = pd.Timedelta(seconds=1)
    topic_b_index = pd.date_range(start=bag_start + offset, end=bag_end, freq=frequency)
    topic_b_data = np.cos(np.linspace(0, 2*np.pi, len(topic_b_index)))
    df_topic_b = pd.DataFrame({"value": topic_b_data}, index=topic_b_index)

    synchronized_topics = {
        "Topic_A": df_topic_a,
        "Topic_B": df_topic_b
    }
    
    final_df = create_final_timeseries(bag_start, bag_end, synchronized_topics, frequency=frequency)
    print(final_df.head(15))
    print(final_df.tail(15))
    
    # Plot the final timeseries.
    import matplotlib.pyplot as plt
    plt.figure(figsize=(12, 6))
    plt.plot(final_df["Duration"], final_df["Topic_A"], label="Topic_A")
    plt.plot(final_df["Duration"], final_df["Topic_B"], label="Topic_B")
    plt.xlabel("Duration (seconds)")
    plt.ylabel("Values")
    plt.title("Unified Time-Series at 100 Hz")
    plt.legend()
    plt.grid(True)
    plt.show()