# steps to test
1. Create a file in the same folder as `app.py` called `config.toml` with the following contents:

2. Start docker on your machine

# build the docker container
    this will build the container from the `Dockerfile` settings which uses `requirements.txt` file to know what to install for python

    ```
    docker build -t my-python-app .
    ```
# run the docker container
    ```
    docker run my-python-app
    ```
