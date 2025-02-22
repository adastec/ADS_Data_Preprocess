import os
import glob

def find_rosbag_files(directory=os.path.expanduser("/workspace")):
    print("Checking directory:", directory)
    print("Directory exists?", os.path.exists(directory))
    pattern = os.path.join(directory, "*.bag")
    print("Using glob pattern:", pattern)
    file_list = glob.glob(pattern)
    print("Found files:", file_list)
    return file_list

def select_rosbag_file(file_list):
    """
    Presents the found rosbag files and allows the user to select one.
    Returns the selected file path, or None if no valid selection is made.
    """
    if not file_list:
        print("No rosbag files found in the directory.")
        return None

    print("Found the following rosbag files:")
    for idx, file in enumerate(file_list, 1):
        print(f"{idx}. {os.path.basename(file)}")

    selection = input("Enter the number of the file you want to process (or 'q' to quit): ")

    if selection.lower() == 'q':
        return None

    try:
        idx = int(selection) - 1
        if 0 <= idx < len(file_list):
            return file_list[idx]
        else:
            print("Invalid selection number.")
            return None
    except ValueError:
        print("Invalid input; please enter a valid number.")
        return None

if __name__ == "__main__":
    # Change the directory if needed, for example, if using a mounted volume.
    rosbag_dir = "/workspace/rosbags"
    files = find_rosbag_files(rosbag_dir)
    selected_file = select_rosbag_file(files)
    if selected_file:
        print(f"You selected: {selected_file}")
        # Here, you would pass the selected file to your data extraction module.
    else:
        print("No rosbag file was selected.")
