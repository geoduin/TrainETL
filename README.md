# Documentation of TrainETL project

## Links:
- [Technical documentation](documentation/solution.md)
- [Test cases](documentation/test_doc.md)
- [Installation manual](documentation/InstallationAstro.md)

## Required software
- Docker
- Astro CLI
- Python
- Snowflake database

## Installing Astro CLI and Docker
In order to run this project, it requires the Astro CLI and Docker.
Follow the installation manual following this link: [Installation manual](documentation/InstallationAstro.md)

## Running Docker compose
This project also runs some Postgres databases, outside of the Airflow docker containers. For this reason a separate docker compose has been written to automatically create Postgres database servers.
Run these commands to create all of the databases:

1. Navigate to the directory containing the `docker-compose.yaml` file:
    ```sh
    cd /path/to/your/project
    ```

2. Start the Docker services defined in the `docker-compose.yml` file:
    ```sh
    docker-compose up -d
    ```

3. To stop the Docker services, run:
    ```sh
    docker-compose down
    ```
## Running Airflow with the Astro CLI.
This section provides detailed instructions on how to set up and run Apache Airflow using the Astro CLI. It covers the initialization of a new Airflow project, starting the Airflow services, and accessing the Airflow web interface. Additionally, it includes commands for stopping and restarting Airflow, as well as running unit tests to ensure the project is functioning correctly.
### 1. Initialize an Airflow Project

1. Create a new directory for your Airflow project:
    ```sh
    mkdir my-airflow-project
    cd my-airflow-project
    ```
2. Initialize the project with Astronomer CLI:
    ```sh
    astro dev init
    ```

### 2. Start Airflow

1. Start the Airflow services using Docker:
    ```sh
    astro dev start
    ```
2. Access the Airflow web interface by navigating to `http://localhost:8080` in your web browser.

### 3. Stop or Restarting Airflow via Astro

* Stopping Airflow:
    ```sh
    astro dev stop
    ```
* Restarting Airflow:
    ```sh
    astro dev restart
    ```

### 4. Executing 

* Stopping Airflow:
    ```sh
    astro dev stop
    ```
* Restarting Airflow:
    ```sh
    astro dev restart
    ```

### 5. Testing
In order to execute unit-test within this project. You need to run this command:

```sh
astro dev pytest
```

## Conclusion
Congrats on installing this project. Have fun!