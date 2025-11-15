# Use Ubuntu 18.04 as a base image, specifying the architecture
FROM --platform=linux/amd64 ubuntu:18.04

# Use a different mirror for fetching packages
RUN sed -i 's|http://archive.ubuntu.com/ubuntu/|http://us.archive.ubuntu.com/ubuntu/|g' /etc/apt/sources.list

# Update and install basic tools, with --fix-missing option
RUN apt-get update -y && apt-get upgrade -y && apt-get install -y --fix-missing \
    curl \
    git \
    python3.8 \
    python3.8-dev \
    python3.8-distutils \
    python3-pip \
    openjdk-8-jdk \
    unixodbc-dev \
    gnupg2 \
    apt-transport-https \
    build-essential \
    libpq-dev \
    software-properties-common

# Set Python 3.8 as the default python3
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.8 1

# Add the Microsoft repository and install the ODBC driver
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/ubuntu/18.04/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17 \
    && ACCEPT_EULA=Y apt-get install -y mssql-tools

# Update PATH environment variable
RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bash_profile \
    && echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc \
    && . ~/.bashrc

# Set the working directory in the container
WORKDIR /usr/src/app

#ENV DBT_LOG_LEVEL=debug

# Copy the application code to the container
COPY matrix/ matrix/
COPY common/ common/
COPY dbtcdwarehouse/ dbtcdwarehouse/

# Install Cython and pyarrow first
RUN pip3 install --upgrade pip setuptools wheel
RUN pip3 install Cython
RUN pip3 install pyarrow

# Install requirements for matrix
WORKDIR /usr/src/app/matrix
RUN pip install --no-cache-dir -r requirements.txt

# Install requirements for dbtcdwarehouse
WORKDIR /usr/src/app/dbtcdwarehouse
RUN pip install --no-cache-dir -r requirements.txt

RUN dbt deps

# Clean up APT when done
RUN apt-get clean && rm -rf /var/lib/apt/lists/*


WORKDIR /usr/src/app
# Make port 80 available to the world outside this container
EXPOSE 80


# Specify the Python command to run the application
CMD ["sh", "-c", "python3 -m matrix.load_data_to_bigquery_update && python3 -m matrix.data_comparison_and_clean_up && python3 -m dbtcdwarehouse.scripts.matrixdbt"]
