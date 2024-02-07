# Health_ETL
# Project Name

Welcome to the Project! This README will guide you through setting up the project environment and running the necessary steps.

## Prerequisites

- Docker installed on your machine
- Python installed on your machine
- Terminal or Command Prompt

## Setup Instructions

1. **Clone the Repository:**
   - Clone the project repository using Git:
     ```bash
     git clone (https://github.com/youssefhamila/Health_ETL.git
     ```

2. **Open Terminal in Project Directory:**
   - Navigate to the project directory using the terminal or command prompt:
     ```bash
     cd Health_ETL
     ```

3. **Install Requirements:**
   - Run the following command to install Python dependencies from `requirements.txt`:
     ```bash
     pip install -r requirements.txt
     ```

## Running the Project

1. **Start Docker Containers:**
   - Execute the following command to start the Docker containers defined in `docker-compose.yml`, which will prepare Postgres database and the flask API:
     ```bash
     docker-compose up -d
     ```

2. **Run ETL Script:**
   - Run the ETL script using the following command:
     ```bash
     python src/etl.py
     ```

3. **Check Results:**
   - Open your web browser and navigate to [http://127.0.0.1:5000/read/first-chunk](http://127.0.0.1:5000/read/first-chunk) to check the results.

## Running Unit Tests

- To run the unit tests, execute the following command:
  ```bash
  python -m unittest discover -s tests/
