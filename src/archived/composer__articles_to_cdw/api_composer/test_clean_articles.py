import unittest
import pandas as pd
from .clean_articles import add_columns

class TestAddColumns(unittest.TestCase):

    def test_add_columns(self):
        # Create a sample dataframe
        data = {
            'author': ['Alice', 'Bob'],
            'content_id': ['1', '2'],
            'word_count': ['500', '1000'],
            'image_count': ['3', '4'],
            'video_count': ['2', '1'],
            'advertisement': [True, False],
            'sponsored': [False, True],
            'promoted_flag': [True, False],
            'comments_flag': [True, True],
            'home_flag': [False, False],
            'published_dts': ['2020-01-01', '2020-01-02']
        }
        df = pd.DataFrame(data)

        # Call the add_columns function
        processed_df = add_columns(df)

        # Assertions to check the output
        self.assertEqual(len(processed_df.columns), 17)
        self.assertIn('hash_key', processed_df.columns)
        self.assertIn('hash_diff', processed_df.columns)
        self.assertIn('record_source', processed_df.columns)
        self.assertIn('sequence_nr', processed_df.columns)
        self.assertIn('record_load_dts_utc', processed_df.columns)
        self.assertIn('load_dt', processed_df.columns)

        # Check author uppercase
        self.assertEqual(processed_df.loc[0, 'author'], 'ALICE')
        self.assertEqual(processed_df.loc[1, 'author'], 'BOB')

        # Check integer conversion
        self.assertEqual(processed_df.loc[0, 'content_id'], 1)
        self.assertEqual(processed_df.loc[0, 'word_count'], 500)
        self.assertEqual(processed_df.loc[0, 'image_count'], 3)
        self.assertEqual(processed_df.loc[0, 'video_count'], 2)

        # Check boolean to integer conversion
        self.assertEqual(processed_df.loc[0, 'advertisement'], 1)
        self.assertEqual(processed_df.loc[1, 'advertisement'], 0)
        self.assertEqual(processed_df.loc[0, 'sponsored'], 0)
        self.assertEqual(processed_df.loc[1, 'sponsored'], 1)
        self.assertEqual(processed_df.loc[0, 'promoted_flag'], 1)
        self.assertEqual(processed_df.loc[1, 'promoted_flag'], 0)
        self.assertEqual(processed_df.loc[0, 'comments_flag'], 1)
        self.assertEqual(processed_df.loc[1, 'comments_flag'], 1)
        self.assertEqual(processed_df.loc[0, 'home_flag'], 0)
        self.assertEqual(processed_df.loc[1, 'home_flag'], 0)
