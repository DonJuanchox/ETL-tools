import unittest
import pathlib
import polars as pl
from unittest.mock import Mock
from etl_tools import load_file, write_parquet, read_parquet

class TestETLTools(unittest.TestCase):
    def setUp(self):
        self.logger = Mock()  # Mock logger to capture log messages
        self.file_path = pathlib.Path('test.csv')
        self.parquet_path = pathlib.Path('test.parquet')
        self.df = pl.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
    
    def test_load_file_valid(self):
        # Test loading a valid CSV file
        result = load_file(self.logger, self.file_path)
        self.logger.info.assert_called_with(f'Loading file {self.file_path}.')
        self.assertIsInstance(result, pl.DataFrame)
    
    def test_write_parquet_valid(self):
        # Test writing to a parquet file
        write_parquet(self.logger, self.df, self.parquet_path)
        self.logger.info.assert_called_with(f'Writing file {self.parquet_path}.')
    
    def test_read_parquet_valid(self):
        # Test reading a valid parquet file
        result = read_parquet(self.logger, self.parquet_path)
        self.logger.info.assert_not_called()
        self.assertIsInstance(result, pl.DataFrame)

if __name__ == '__main__':
    unittest.main()
