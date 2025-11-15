# steps to test
1. create a file in the same folder as `app.py` called `config.toml` with the following contents:

    ```toml
    [sql_server_cdw]
    host = "{hostname of sqlserver instance}"
    database = "{database}"
    username = "{username}"
    password = "{password}"
    ```

2. start docker on your machine

# build the docker container
    this will build the container from the `Dockerfile` settings which uses `requirements.txt` file to know what to install for python

    ```
    docker build -t my-python-app .
    ```
# run the docker container
    ```
    docker run -it --rm --network="host" -v $(pwd):/app --name my-running-app my-python-app-test
    ```
