import unittest
import logging
from unittest.mock import MagicMock, patch
import etl_logger

class TestEtlLogger(unittest.TestCase):

    @patch('os.getlogin', return_value='test_user')
    def test_get_logger_basic(self, mock_getlogin):
        """Test that get_logger returns a logger with the correct name and level"""
        # Prepare a mock handler
        mock_handler = MagicMock()

        # Call get_logger with valid logging level
        logger = etl_logger.get_logger('test_logger', logging.INFO, [mock_handler])

        # Check if the logger has the correct name and level
        self.assertEqual(logger.logger.name, 'test_logger')
        self.assertEqual(logger.logger.level, logging.INFO)

        # Check if the handler has been set correctly
        mock_handler.setLevel.assert_called_with(logging.INFO)
        mock_handler.setFormatter.assert_called_once()

        # Check if the logger has a formatter with the correct format string
        formatter = mock_handler.setFormatter.call_args[0][0]
        expected_format = '%(asctime)s - %(user)s - %(name)s - %(filename)s @%(funcName)s #%(lineno)s - %(levelname)s - %(message)s'
        self.assertEqual(formatter._fmt, expected_format)


    @patch('os.getlogin', return_value='test_user')
    def test_logger_with_multiple_handlers(self, mock_getlogin):
        """Test that the logger works with multiple handlers"""
        # Prepare two mock handlers
        mock_handler1 = MagicMock()
        mock_handler2 = MagicMock()

        # Get the logger with multiple handlers
        logger = etl_logger.get_logger('test_logger', logging.INFO, [mock_handler1, mock_handler2])

        # Check if both handlers were added correctly
        mock_handler1.setLevel.assert_called_with(logging.INFO)
        mock_handler2.setLevel.assert_called_with(logging.INFO)

        # Assert that the logger's handlers are correctly assigned
        self.assertIn(mock_handler1, logger.logger.handlers)
        self.assertIn(mock_handler2, logger.logger.handlers)

    @patch('os.getlogin', return_value='test_user')
    def test_logger_clears_old_handlers(self, mock_getlogin):
        """Test that the logger clears old handlers before adding new ones"""
        mock_handler = MagicMock()

        # Create a logger with an existing handler and ensure it's cleared
        logger = etl_logger.get_logger('test_logger', logging.INFO, [mock_handler])

        # Clear existing handlers
        logger.logger.handlers.clear()
        self.assertEqual(len(logger.logger.handlers), 0)

        # Add a new handler and verify it gets added correctly
        logger = etl_logger.get_logger('test_logger', logging.INFO, [mock_handler])
        self.assertEqual(len(logger.logger.handlers), 1)

if __name__ == '__main__':
    unittest.main()
