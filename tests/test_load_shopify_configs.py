import unittest
import pandas as pd
import numpy as np
import os

import dags.load_shopify_configs as load_shopify_configs

NAN = float('NaN')
CSV_PATH = os.path.join(os.path.dirname(__file__), "fixtures/test.csv")

class TestLoadShopifyConfigs(unittest.TestCase):
    def test_transform_data(self):
        """ Test the transformation logic. 
        
        We check that records with undefined application_id are dropped 
        and the index_prefix column is correctly populated
        """

        input_df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'application_id': ['A', NAN, 'C', 'D', 'E'],
            'index_prefix': ['shopify_', 'shopify_', 'shopify_similar_', 'other', NAN]
        })
        
        output_df = load_shopify_configs.transform(input_df)
        
        expected_output_df = pd.DataFrame({
            'id': [1, 3, 4, 5],
            'application_id': ['A', 'C', 'D', 'E'],
            'index_prefix': ['shopify_', 'shopify_similar_', 'other', NAN],
            'has_specific_prefix': [False, True, True, True]
        })
        
        pd.testing.assert_frame_equal(output_df.reset_index(drop=True), expected_output_df)
        
    
    def test_load_csv(self):
        """ Test the loading of the CSV file as pandas dataframe.
        
        We test that all columns and rows are loaded, and that empty values are correctly handled.
        """

        df = pd.read_csv(CSV_PATH)
        
        assert df.shape == (3, 22)
        
        expected_df = pd.DataFrame({
            'id': ['i0', 'i1', 'i2'],
            'application_id': ['DHOTN807YZ', NAN, 'D6WMGCDWN5'],
            'index_prefix': ['shopify_', 'shopify_similar_', NAN]
        })
        
        pd.testing.assert_frame_equal(df[['id', 'application_id', 'index_prefix']], expected_df)


if __name__ == '__main__':
    unittest.main()
