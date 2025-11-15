# Use an official Python runtime (slim version) as a parent image
FROM python:3.12-slim

# Set the working directory in the container to /app
WORKDIR /usr/src/app

# Display the current working directory
RUN pwd

# Copy the contents of the brightcove-workflow directory into /usr/src/app
COPY qualtrics qualtrics
COPY dbtcdwarehouse dbtcdwarehouse
COPY common common
COPY requirements-docker.txt requirements-docker.txt

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements-docker.txt

WORKDIR /usr/src/app/dbtcdwarehouse

RUN dbt deps

WORKDIR /usr/src/app

# Make port 80 available to the world outside this container
EXPOSE 80

# Specify the Python command to run the application
CMD ["python", "-m", "qualtrics.qualtrics__news_hub"]
