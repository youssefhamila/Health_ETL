from flask import Flask, jsonify
import psycopg2
import os
from logger import Logger


class HealthAPI(Flask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.setup_routes()
        self.setup_logger()

    def setup_routes(self):
        self.route('/read/first-chunk', methods=['GET'])(self.get_first_chunk)

    def setup_logger(self):
        self.etl_logger = Logger('logs/flask_app.log')
        self.logger = self.etl_logger.get_logger()

    def fetch_data_from_database(self):
        # Fetch first 10 rows from the database
        connection = psycopg2.connect(user=os.getenv("POSTGRES_USER"),
                                      password=os.getenv("POSTGRES_PASSWORD"),
                                      host="localhost",
                                      port="5432",
                                      database=os.getenv("POSTGRES_DB"))
        cursor = connection.cursor()

        cursor.execute("SELECT * FROM sleep_health LIMIT 10")
        data = cursor.fetchall()
        column_names = [description[0] for description in cursor.description]
        connection.close()
        return column_names, data

    def get_first_chunk(self):
        # API endpoint
        try:
            column_names, data = self.fetch_data_from_database()
            response_data = [{column_names[i]: value for i, value in enumerate(row)} for row in data]
            self.logger.info('API request successful: 10 rows returned')
            return jsonify(response_data)
        except Exception as e:
            self.logger.error('Table {} not created: {}'.format(os.getenv("POSTGRES_DB"), e))
            return jsonify([{'error': 'Table {} not created: run ETL'.format(os.getenv("POSTGRES_DB"))}]), 503


if __name__ == '__main__':
    app = HealthAPI(import_name=__name__)
    app.run()
