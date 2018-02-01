import os
import sys
import json
import requests


class TransmartApiException(Exception):
    pass

class TransmartApiCalls(object):

    def __init__(self, url, username, password, rebuild_timeout):
        self.url = url
        self.username = username
        self.password = password
        self.token = None
        self.cache_rebuild_timeout = float(rebuild_timeout)

    def get_token(self):
        """
        Returns the access token for accessing the server.
        Retrieves a token from the server, if that has not been done earlier this session.
        :return: the access token.
        """
        if self.token is None:
            self.token = self.retrieve_token()
        return self.token

    def retrieve_token(self):
        """
        Retrieve access token from the server.
        """
        headers = {'Accept': 'application/json'}
        url = self.url + '/oauth/token'
        params = {
            'grant_type': 'password',
            'client_id': 'glowingbear-js',
            'client_secret': '',
            'username': self.username,
            'password': self.password
        }
        try:
            response = requests.post(url, params, headers=headers)
            if not response.ok:
                response.raise_for_status()
            data = response.json()
            token = data['access_token']
        except Exception as e:
            raise TransmartApiException('Could not retrieve access token for %s: %s' % (url, e))

        return token


    def scan_subscription_queries(self):
        """
        Triggers the scan of stored set queries in TranSMART.

        """
        self.post('/v2/query_sets/scan')


    def rebuild_tree_nodes_cache(self):
        """
        Triggers a rebuild of the tree nodes cache of TranSMART.
        Waits max `cache_rebuild_timeout` seconds for the rebuild to finish
        before returning.
        """

        Console.info('Clearing tree nodes cache ...')
        self.get('/v2/tree_nodes/clear_cache')


    def retrieve_cache_rebuild_status(self):
        try:
            data = self.get_json('/v2/tree_nodes/rebuild_status')
            return data['status']
        except TransmartApiException as e:
            Console.warning(e)
            return None


    def get_json(self, path):
        """
        Retrieves JSON data from the server.
        :param path: the API path to fetch data from.
        :return: the JSON data.
        """
        response = self.get(path)
        return response.json()


    def get(self, path):
        """
        Performs a call to the server.
        :param path: the API path to call.
        :return: the response.
        """
        token = self.get_token()
        headers = {
            'Accept': 'application/json',
            'Authorization': 'Bearer ' + str(token)
        }
        url = self.url + path
        response = None
        Console.warning(url)
        try:
            response = requests.get(url, headers=headers)
            if not response.ok:
                response.raise_for_status()
            return response
        except Exception as e:
            Console.error('Retrieving %s failed: %s' % (path, response))
            raise TransmartApiException(e)


    def post(self, path):
        """
        Performs a post call to the server
        :param path: the API path to call
        :return: the response.
        """
        token = self.get_token()
        headers = {
            'Accept': 'application/json',
            'Authorization': 'Bearer ' + str(token)
        }
        url = self.url + path
        response = None
        try:
            response = requests.post(url, headers=headers)
            if not response.ok:
                response.raise_for_status()
            return response
        except Exception as e:
            Console.error('Retrieving %s failed: %s' % (path, response))
            raise TransmartApiException(e)


class Console:
    """
    A helper class for displaying messages on the console (stderr).
    """

    Black = '\033[30m'
    BlackBackground = '\033[40m'
    Blue = '\033[94m'
    Green = '\033[92m'
    GreenBackground = '\033[42m'
    Yellow = '\033[93m'
    YellowBackground = '\033[103m'
    Red = '\033[91m'
    RedBackground = '\033[41m'
    Grey = '\033[37m'
    Reset = '\033[0m'

    @staticmethod
    def title(title):
        print('%s%s%s' % (Console.Blue, title, Console.Reset), file=sys.stderr)

    @staticmethod
    def success(message):
        print('%s%s%s' % (Console.Green, message, Console.Reset), file=sys.stderr)

    @staticmethod
    def error(message):
        print('%s%sError%s%s: %s%s' %
              (Console.RedBackground, Console.Black, Console.Reset, Console.Grey, message, Console.Reset),
              file=sys.stderr)

    @staticmethod
    def warning(message):
        print('%s%sWarning%s%s: %s%s' %
              (Console.YellowBackground, Console.Black, Console.Reset, Console.Grey, message, Console.Reset),
              file=sys.stderr)

    @staticmethod
    def info(message):
        print(message, file=sys.stderr)