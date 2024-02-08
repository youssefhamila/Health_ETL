import pandas as pd
from sqlalchemy import create_engine, inspect
from logger import Logger
import sys


# Configuration
DATABASE_URI = 'postgresql://postgres:etl_test@localhost/health_db'
TABLE_NAME = 'sleep_health'
etl_logger = Logger('logs/etl.log')
logger = etl_logger.get_logger()


# Extract data from one or multiple csv files
def extract(csv_files):
    logger.info("Loading data from {} datasets".format(len(csv_files)))
    dfs = []
    for file in csv_files:
        logger.info("Extracting data from {}".format(file))
        try:
            df = pd.read_csv(file)
            dfs.append(df)
            logger.info("Data from {} extracted successfully".format(file))
        except Exception as e:
            logger.error("Error extracting data from {}: {}".format(file, e))
    logger.info("Data loaded.")
    return pd.concat(dfs)


# Returns BMI category based in calculated DMI and gender
def categorize_bmi(gender, bmi):
    if gender == 'Male':
        if bmi < 18.5:
            return 'Underweight'
        elif bmi < 25:
            return 'Normal weight'
        elif bmi < 30:
            return 'Overweight'
        else:
            return 'Obese'
    elif gender == 'Female':
        if bmi < 18.5:
            return 'Underweight'
        elif bmi < 24:
            return 'Normal weight'
        elif bmi < 29:
            return 'Overweight'
        else:
            return 'Obese'
    else:
        return 'Unknown'


# define sleep quality based on  duration of sleep (hours)
def categorize_sleep_quality(hours):
    if hours >= 7:
        return 'Good'
    elif 6 <= hours < 7:
        return 'Fair'
    else:
        return 'Poor'


# Apply all transformation functions to the dataset
def transform(df):
    logger.info("Transforming data")
    try:
        df['BMI'] = df['Weight'] / ((df['Height'] / 100) ** 2)
        logger.info("Column BMI added")
        df['BMI category'] = df.apply(lambda row: categorize_bmi(row['Gender'], row['BMI']), axis=1)
        logger.info("Column BMI category added")
        df['Sleep quality'] = df['Sleep Duration'].apply(categorize_sleep_quality)
        logger.info("Column Sleep_quality added")
        return df.sort_index(axis=1)
    except Exception as e:
        logger.error("Error transforming data: {}".format(e))


# Create table sleep_health in case it doesn't exist already
def create_table(engine, table_name, data):
    inspector = inspect(engine)
    if table_name not in inspector.get_table_names():
        try:
            data.head(0).to_sql(table_name, engine, if_exists='replace', index=False)
            logger.info(f"Table '{table_name}' created.")
        except Exception as e:
            logger.error("Error creating table '{}': {}".format(table_name, e))
    else:
        logger.info(f"Table '{table_name}' already exists.")


# insert data into the database
def insert_data(data, table_name, db_uri):
    engine = create_engine(db_uri)
    create_table(engine, table_name, data)
    try:
        data.to_sql(table_name, engine, if_exists='append', index=False)
        logger.info("Inserted {} rows into '{}' successfully.".format(len(data), table_name))
    except Exception as e:
        logger.error("Error loading data into '{}': {}".format(table_name, e))


def main(csv_files):
    try:
        data = extract(csv_files)
        transformed_data = transform(data)
        insert_data(transformed_data, TABLE_NAME, DATABASE_URI)
    except Exception as e:
        logger.error("ETL process failed: {}".format(e))


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Datasets missing: python script_name.py file1.csv file2.csv")
        sys.exit(1)
    csv_files = sys.argv[1:]
    main(csv_files)
