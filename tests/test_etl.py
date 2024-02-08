import unittest
import pandas as pd
import sys
sys.path.append('src')
from etl import transform


# test ETL transformations
class TestETL(unittest.TestCase):
    def setUp(self):
        self.test_df = pd.DataFrame({
            'Gender': ['Male'],
            'Height': [153.6],
            'Weight': [60],
            'Sleep Duration': [8]
        })

    def test_transform(self):
        transformed_data = transform(self.test_df)
        self.assertEqual(transformed_data.shape, (1, 7))
        self.assertTrue('BMI' in transformed_data.columns)
        self.assertTrue('BMI_category' in transformed_data.columns)
        self.assertTrue('Sleep_quality' in transformed_data.columns)
        self.assertEqual(transformed_data['BMI'].iloc[0], 25.431315104166664)
        self.assertEqual(transformed_data['BMI category'].iloc[0], 'Overweight')
        self.assertEqual(transformed_data['Sleep_quality'].iloc[0], 'Good')


if __name__ == '__main__':
    unittest.main()
