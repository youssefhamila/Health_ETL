import unittest
import logging
from src.logger import Logger
import os


class TestLogger(unittest.TestCase):
    # Test if the logger is working correctly
    def test_logger_creation(self):
        logger = Logger("logs/test.log").get_logger()
        self.assertIsInstance(logger, logging.Logger)
        if os.path.exists("logs/test.log"):
            os.remove("logs/test.log")

    def test_logging(self):
        logger = Logger("logs/test.log").get_logger()
        logger.info("Test message")
        with open("logs/test.log", "r") as file:
            log_content = file.read()
            self.assertIn("Test message", log_content)


if __name__ == "__main__":
    unittest.main()
