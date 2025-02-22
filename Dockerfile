FROM ros:noetic

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-venv \
    git \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install Python libraries
RUN pip3 install --upgrade pip && \
    pip3 install numpy pandas pyarrow h5py matplotlib pytest

# Set the working directory
WORKDIR /workspace

# Default command
CMD ["bash"]
