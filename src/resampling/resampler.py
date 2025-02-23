import pandas as pd

def convert_to_dataframe(topic_data):
    """
    Converts a list of (timestamp, message) tuples into a Pandas DataFrame.
    Assumes the message is a numeric value.
    """
    df = pd.DataFrame(topic_data, columns=["timestamp", "value"])
    # Convert the timestamp (assumed to be in seconds) to a datetime index.
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
    df.set_index("timestamp", inplace=True)
    return df

def resample_dataframe(df, frequency="10L"):
    """
    Resamples the DataFrame to the given frequency (default 10 milliseconds for 100 Hz)
    and uses linear interpolation to fill in missing values.
    """
    # Resample and compute the mean value for each interval,
    # then use interpolation to fill gaps.
    df_resampled = df.resample(frequency).mean().interpolate(method="linear")
    return df_resampled

def synchronize_topics(data_dict, frequency="10L"):
    """
    Given a dictionary mapping topic names to lists of (timestamp, message) tuples,
    converts each list to a DataFrame, resamples them, and returns a dictionary
    mapping each topic to its synchronized DataFrame.
    """
    synchronized = {}
    for topic, data in data_dict.items():
        df = convert_to_dataframe(data)
        df_resampled = resample_dataframe(df, frequency)
        synchronized[topic] = df_resampled
    return synchronized

if __name__ == "__main__":
    # Example: simulate data for one topic
    import time, random
    current_time = time.time()
    # Simulate 10 data points with a roughly 50ms interval and a little jitter
    topic_data = [
        (current_time + i * 0.05 + random.uniform(-0.005, 0.005), random.random())
        for i in range(10)
    ]
    data_dict = {"/topic1": topic_data}
    synchronized_data = synchronize_topics(data_dict)
    
    print("Synchronized data for /topic1:")
    print(synchronized_data["/topic1"].head(20))
