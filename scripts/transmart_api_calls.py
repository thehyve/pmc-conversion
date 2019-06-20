import os
import sys
import json
import requests
import asyncio


class TransmartApiException(Exception):
    pass

class TransmartApiCalls(object):

    def __init__(self, keycloak_url, transmart_url, gb_backend_url, client_id, offline_token):
        self.url = keycloak_url
        self.token = None
        self.tm_url = transmart_url
        self.gb_backend_url = gb_backend_url
        self.client_id = client_id
        self.offline_token = offline_token

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
        Retrieve an access token from Keycloak based on the client_id and the offline token,
        which is stored in the config.
        """
        headers = {'Accept': 'application/json',
                   'Contect-Type': 'application/x-www-form-urlencoded'
                   }
        url = self.url + '/protocol/openid-connect/token'
        params = {
            'grant_type': 'refresh_token',
            'scope': 'offline_access',
            'client_id': self.client_id,
            'refresh_token': self.offline_token
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
        Triggers the scan of stored set queries in Gb Backend app.

        """
        self.post('/queries/sets/scan', server_url=self.gb_backend_url)


    def clear_tree_nodes_cache(self):
        """
        Triggers a clear of the tree nodes cache of TranSMART.
        """

        Console.info('Clearing tree nodes cache ...')
        self.get('/v2/tree_nodes/clear_cache')


    def rebuild_tree_cache(self):
        """
        Triggers a rebuild of the tree nodes cache of TranSMART.
        Waits max `cache_rebuild_timeout` seconds for the rebuild to finish
        before returning.
        """
        Console.info('Rebuilding tree nodes cache ...')
        self.get('/v2/tree_nodes/rebuild_cache')


    def after_data_loading(self):
        """
        Triggers a clear of the caches of TranSMART and scans for query subscriptions
        """
        Console.info('After data loading update, clearing and rebuilding caches.')
        self.get('/v2/admin/system/after_data_loading_update')


    def update_status(self):
        """
        Gets a status report about the current after data loading update task
        """
        Console.info('After data loading update status check.')
        response = self.get('/v2/admin/system/update_status')
        return response.json()


    async def check_status(self, n, sleep=30.0):
        """
        Checks the after data loading task status periodically every $sleep seconds
        Waits max `sleep*n` seconds for the status to be `COMPLETED` or `FAILED`
        :param n: max status call retrials
        :param sleep: number of seconds before update_status is called again, default: 30s.
        """
        for i in range(n):
            update_status = self.update_status()
            if update_status['status'] == 'COMPLETED':
                return
            elif update_status['status'] == 'FAILED':
                Console.error('After data loading update failed. Error: %s' % (update_status['message']))
                raise TransmartApiException('After data loading update failed. Error: %s' % (update_status['message']))
            Console.info('%s/%s Current status of the update: %s. Sleeping for %s seconds ...' %
                         (i, n, update_status['status'], sleep))
            await asyncio.sleep(sleep)
        Console.error('After data loading update took too long: %s seconds. Transmart Api task interrupted.' % (n*sleep))
        raise TransmartApiException('Timeout. Not able to finish an update task within %s seconds.' % (n*sleep))


    def get(self, path, **kwargs):
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
        url = kwargs.get('server_url', self.tm_url) + path
        response = None
        Console.info('Making a get call to: %s' % url)
        try:
            response = requests.get(url, headers=headers)
            if not response.ok:
                response.raise_for_status()
            return response
        except Exception as e:
            Console.error('Retrieving %s failed: %s' % (url, response))
            raise TransmartApiException(e)


    def post(self, path, **kwargs):
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
        url = kwargs.get('server_url', self.tm_url) + path
        response = None
        Console.info('Making a post call to: %s' % url)
        try:
            response = requests.post(url, headers=headers)
            if not response.ok:
                response.raise_for_status()
            return response
        except Exception as e:
            Console.error('Retrieving %s failed: %s' % (url, response))
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