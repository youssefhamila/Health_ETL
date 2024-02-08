import unittest
import json
import requests


class TestFlaskApp(unittest.TestCase):
    # Test API respsonse code, data type and number of elements
    def test_get_first_chunk(self):
        response = requests.get("http://localhost:5000/read/first-chunk")
        self.assertIn(response.status_code, [200, 503])
        data = json.loads(response.content)
        self.assertTrue(isinstance(data, list))
        self.assertTrue(len(data) <= 10)


if __name__ == '__main__':
    unittest.main()
