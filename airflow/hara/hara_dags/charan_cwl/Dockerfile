# Use the official Python base image
FROM python:3.9

# Set the working directory
WORKDIR /app

# Copy the script into the Docker image
COPY ExtractData.py .

# Install the requests package
RUN pip install requests


# Set the entrypoint to the script
ENTRYPOINT ["python", "/app/ExtractData.py"]
