# Use an official Python runtime as a parent image
FROM python:3.12-slim

# Set the working directory in the container to /app
WORKDIR /usr/src/app

# Display the current working directory
RUN pwd

# Copy the contents of the dbtcdwarehouse and common directory into /usr/src/app

COPY dbtcdwarehouse dbtcdwarehouse

COPY common common

# Install any needed packages specified in requirements.txt
WORKDIR /usr/src/app/dbtcdwarehouse

RUN pip install --no-cache-dir -r requirements.txt

RUN dbt deps

#RUN pip install --no-cache-dir -r /usr/src/app/brightcove-workflow/requirements.txt
WORKDIR /usr/src/app


# Make port 80 available to the world outside this container
EXPOSE 80

# Specify the Python command to run the application
CMD ["python", "-m", "dbtcdwarehouse.scripts.ga4"]
