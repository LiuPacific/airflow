# Use the official Python base image
FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy the script into the Docker image
COPY data_metadata.py .

# Install the requests package
RUN pip install pandas


# Set the entrypoint to the script
ENTRYPOINT ["python", "/app/data_metadata.py"]
