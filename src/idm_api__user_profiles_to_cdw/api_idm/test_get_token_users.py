'''import requires'''
import unittest
from unittest import mock
from .get_token_users import get_token

class TestGetToken(unittest.TestCase):
    '''Test get_token function'''
    def test_get_token(self):
        '''Test get_token function'''
        # Define mock response
        mock_response = mock.Mock()
        mock_response.json.return_value = {'token': 'abcdefg'}

        # Patch requests.get to return mock response
        with mock.patch('requests.get', return_value=mock_response):
            # Call function with mock arguments
            token = get_token('http://example.com', 'username', 'password', 'auth')

            # Check if token is a dictionary containing the expected key
            self.assertIsInstance(token, dict)
            self.assertIn('token', token)
