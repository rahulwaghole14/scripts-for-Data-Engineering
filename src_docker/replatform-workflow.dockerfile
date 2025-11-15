# Use an official Python runtime as a parent image
FROM python:3.12-slim

# Set the working directory in the container to /app
WORKDIR /usr/src/app

# Display the current working directory
RUN pwd

# Copy the contents of the brightcove-workflow directory into /usr/src/app

COPY replatform replatform

COPY replatform/common common

COPY requirements-docker.txt requirements-docker.txt

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements-docker.txt


# Make port 80 available to the world outside this container
EXPOSE 80

# Specify the Python command to run the application

CMD ["python", "-m", "replatform.main"]
