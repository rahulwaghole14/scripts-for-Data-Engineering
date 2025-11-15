# Use an official Python runtime as a parent image
FROM python:3.12-slim

# Set the working directory in the container to /app
WORKDIR /usr/src/app

# Install Node.js
RUN apt-get update && apt-get install -y nodejs npm

# Install crypto-js
RUN npm install crypto-js

# Display the current working directory
RUN pwd

# Copy the contents of the idm-bigquery directory into /usr/src/app
COPY idm-bigquery idm-bigquery

#COPY common files
COPY common common

COPY dbtcdwarehouse dbtcdwarehouse

WORKDIR /usr/src/app/idm-bigquery

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /usr/src/app/dbtcdwarehouse
RUN dbt deps

WORKDIR /usr/src/app
# Make port 80 available to the world outside this container
EXPOSE 80

# Specify the Python command to run the application
CMD ["python", "-m", "idm-bigquery.main"]
