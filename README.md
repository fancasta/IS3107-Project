# IS3107 Airflow Project

This is a Docker-based project for IS3107, which includes an Airflow webserver and a MySQL database. 

## Accessing Containers

To access the containers, you need to have Docker installed on your machine. Once you have Docker installed, follow these steps:

1. Clone this repository to your local machine.
2. Navigate to the project's root directory.
3. Run the following command to start the containers:

`docker-compose up`

This will start both the MySQL database and the Airflow webserver containers. Once the containers are up and running, you can access the Airflow UI by visiting `http://localhost:8080` in your web browser. 

To stop the containers, use the following command:

`docker-compose down`

## Project Structure

The project consists of two services:

- MySQL Database
- Airflow Webserver

The MySQL database is used as the backend for Airflow, and the Airflow webserver is used to manage and monitor the DAGs.

The `docker-compose.yml` file defines the services, ports, and environment variables for the project. The `Dockerfile` and `airflowDockerfile` files define the Docker images used for the Airflow webserver. 

The `./dags` directory contains the DAG files, which are mounted as a volume to the Airflow webserver container.

To initialize the database, create a user account, and start the webserver and scheduler, the `entrypoint` command in the `docker-compose.yml` file runs the following command when the container starts:

<code>airflow db init &&
airflow users create --username admin --password admin --firstname Anonymous --lastname Admin --role Admin --email admin@example.com &&
airflow webserver --port 8080 -D &&
airflow scheduler</code>

This will set up the Airflow environment with a default admin user account, and you can start creating and scheduling DAGs.
