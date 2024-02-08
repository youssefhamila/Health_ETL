import pandas as pd
from sqlalchemy import create_engine, inspect
from logger import Logger
import sys


class ETLProcess:
    def __init__(self, database_uri, table_name, log_file):
        # Initialize ETLProcess with database URI, table name, and log file
        self.database_uri = database_uri
        self.table_name = table_name
        self.etl_logger = Logger(log_file)
        self.logger = self.etl_logger.get_logger()

    def extract(self, csv_files):
        # Extract data from CSV files
        self.logger.info("Loading data from {} datasets".format(len(csv_files)))
        dfs = []
        for file in csv_files:
            self.logger.info("Extracting data from {}".format(file))
            try:
                df = pd.read_csv(file)
                dfs.append(df)
                self.logger.info("Data from {} extracted successfully".format(file))
            except Exception as e:
                self.logger.error("Error extracting data from {}: {}".format(file, e))
        return pd.concat(dfs)

    def categorize_bmi(self, gender, bmi):
        # Categorize BMI based on gender and BMI value
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

    def categorize_sleep_quality(self, hours):
        # Categorize sleep quality based on hours of sleep
        if hours >= 7:
            return 'Good'
        elif 6 <= hours < 7:
            return 'Fair'
        else:
            return 'Poor'

    def transform(self, df):
        # Apply all transformation functions to the dataset
        self.logger.info("Transforming data")
        try:
            df['BMI'] = df['Weight'] / ((df['Height'] / 100) ** 2)
            self.logger.info("Column BMI added")
            df['BMI category'] = df.apply(lambda row: self.categorize_bmi(row['Gender'], row['BMI']), axis=1)
            self.logger.info("Column BMI category added")
            df['Sleep quality'] = df['Sleep Duration'].apply(self.categorize_sleep_quality)
            self.logger.info("Column Sleep quality added")
            return df.sort_index(axis=1)
        except Exception as e:
            self.logger.error("Error transforming data: {}".format(e))

    def create_table(self, engine, table_name, data):
        # Create table if it doesn't exist already
        inspector = inspect(engine)
        if table_name not in inspector.get_table_names():
            try:
                data.head(0).to_sql(table_name,
                                    engine,
                                    if_exists='replace',
                                    index=False)
                self.logger.info(f"Table '{table_name}' created in DB.")
            except Exception as e:
                self.logger.error("Error creating table '{}': {}".format(table_name, e))
        else:
            self.logger.info(f"Table '{table_name}' already exists in DB.")

    def insert_data(self, data, table_name, db_uri):
        # Insert data into the database
        engine = create_engine(db_uri)
        self.create_table(engine, table_name, data)
        try:
            data.to_sql(table_name, engine, if_exists='append', index=False)
            self.logger.info("Inserted {} rows into '{}' successfully.".format(len(data), table_name))
        except Exception as e:
            self.logger.error("Error loading data into '{}': {}".format(table_name, e))

    def main(self, csv_files):
        # Main ETL process
        try:
            data = self.extract(csv_files)
            transformed_data = self.transform(data)
            self.insert_data(transformed_data, self.table_name, self.database_uri)
        except Exception as e:
            self.logger.error("ETL process failed: {}".format(e))


if __name__ == "__main__":
    # Entry point of the script
    if len(sys.argv) < 2:
        print("Datasets missing: python script_name.py file1.csv file2.csv")
        sys.exit(1)
    csv_files = sys.argv[1:]
    etl_process = ETLProcess('postgresql://postgres:etl_test@localhost/health_db',
                             'sleep_health',
                             'logs/etl.log')
    etl_process.main(csv_files)
