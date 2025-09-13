# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the collector script into the container
COPY edgex_data_collector.py .

# Command to run the application
CMD ["python", "./edgex_data_collector.py"]
