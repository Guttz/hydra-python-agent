import logging, sys
import socketio
import urllib.request
from hydra_agent.redis_core.redis_proxy import RedisProxy
from hydra_agent.redis_core.graphutils_operations import GraphOperations
from hydra_agent.redis_core.graph_init import InitialGraph
from hydra_python_core import doc_maker
from typing import Union, Tuple
from requests import Session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)


class Agent(Session, socketio.ClientNamespace, socketio.Client):
    """Provides a straightforward GET, PUT, POST, DELETE -
    CRUD interface - to query hydrus
    """

    def __init__(self, entrypoint_url: str, namespace: str='/sync') -> None:
        """Initialize the Agent
        :param entrypoint_url: Entrypoint URL for the hydrus server
        :param namespace: Namespace endpoint to listen for updates
        :return: None
        """
        self.entrypoint_url = entrypoint_url.strip().rstrip('/')
        self.redis_proxy = RedisProxy()
        self.redis_connection = self.redis_proxy.get_connection()
        Session.__init__(self)
        jsonld_api_doc = super().get(self.entrypoint_url + '/vocab').json()
        self.api_doc = doc_maker.create_doc(jsonld_api_doc)
        self.initialize_graph()
        self.graph_operations = GraphOperations(self.entrypoint_url,
                                                self.api_doc,
                                                self.redis_proxy)

        # Declaring Socket Rules and instaciating Synchronization Socket
        socketio.ClientNamespace.__init__(self, namespace)
        socketio.Client.__init__(self, logger=True)
        socketio.Client.register_namespace(self, self)
        # After hydrus implementation connect directly to the sv, self.entrypoint
        self.entrypoint_url_temp = 'http://localhost:5000'
        socketio.Client.connect(self, self.entrypoint_url_temp,
                                namespaces=namespace)
        self.last_job_id = -1
        self.modification_table = []

    def initialize_graph(self) -> None:
        """Initialize the Graph on Redis based on ApiDoc
        :param entrypoint_url: Entrypoint URL for the hydrus server
        :return: None
        """
        self.graph = InitialGraph()
        self.redis_connection.delete("apigraph")
        self.graph.main(self.entrypoint_url, self.api_doc, True)
        self.redis_connection.sadd("fs:url", self.entrypoint_url)

    def get(self, url: str = None, resource_type: str = None,
            filters: dict = {},
            cached_limit: int = sys.maxsize) -> Union[dict, list]:
        """READ Resource from Server/cached Redis
        :param url: Resource URL to be fetched
        :param resource_type: Resource object type
        :param filters: filters to apply when searching, resources properties
        :param cached_limit : Minimum amount of resources to be fetched
        :return: Dict when one object or a list when multiple targerted objects
        """
        redis_response = self.graph_operations.get_resource(url, resource_type,
                                                            filters)
        if redis_response:
            if type(redis_response) is dict:
                return redis_response
            elif len(redis_response) >= cached_limit:
                return redis_response

        # If querying with resource type build url
        # This can be more stable when adding Manages Block
        # More on: https://www.hydra-cg.com/spec/latest/core/#manages-block
        if resource_type:
            url = self.entrypoint_url + "/" + resource_type + "Collection"
            response = super().get(url, params=filters)
        else:
            response = super().get(url)

        if response.status_code == 200:
            # Graph_operations returns the embedded resources if finding any
            embedded_resources = \
                self.graph_operations.get_processing(url, response.json())
            self.process_embedded(embedded_resources)
            if response.json()['@type'] in self.api_doc.parsed_classes:
                return response.json()
            else:
                return response.json()['members']
        else:
            return response.text

    def put(self, url: str, new_object: dict) -> Tuple[dict, str]:
        """CREATE resource in the Server/cache it on Redis
        :param url: Server URL to create the resource at
        :param new_object: Dict containing the object to be created
        :return: Dict with server's response and resource URL
        """
        response = super().put(url, json=new_object)

        if response.status_code == 201:
            url = response.headers['Location']
            # Graph_operations returns the embedded resources if finding any
            embedded_resources = \
                self.graph_operations.put_processing(url, new_object)
            self.process_embedded(embedded_resources)
            return response.json(), url
        else:
            return response.text, ""

    def post(self, url: str, updated_object: dict) -> dict:
        """UPDATE resource in the Server/cache it on Redis
        :param url: Server URL to update the resource at
        :param updated_object: Dict containing the updated object
        :return: Dict with server's response
        """
        response = super().post(url, json=updated_object)

        if response.status_code == 200:
            # Graph_operations returns the embedded resources if finding any
            embedded_resources = \
                self.graph_operations.post_processing(url, updated_object)
            self.process_embedded(embedded_resources)
            return response.json()
        else:
            return response.text

    def delete(self, url: str) -> dict:
        """DELETE resource in the Server/delete it on Redis
        :param url: Resource URL to be deleted
        :return: Dict with server's response
        """
        response = super().delete(url)

        if response.status_code == 200:
            self.graph_operations.delete_processing(url)
            return response.json()
        else:
            return response.text

    def process_embedded(self, embedded_resources: list) -> None:
        """Helper function to process a list of embedded resources
        fetching and linking them to their parent Nodes
        :param embedded_resources: List of dicts containing resources
        """
        # Embedded resources are fetched and then properly linked
        for embedded_resource in embedded_resources:
            self.get(embedded_resource['embedded_url'])
            self.graph_operations.link_resources(
                embedded_resource['parent_id'],
                embedded_resource['parent_type'],
                embedded_resource['embedded_url'])

    # Below are the functions that are reponsible to process Socket Events
    def on_connect(self):
        logger.info('Socket Connection Established - Synchronization ON')
        self.modification_table = super().get(self.entrypoint_url_temp +
                                              '/modification-table').json()
        if self.modification_table:
            self.last_job_id = self.modification_table[0]['job_id']

    def on_disconnect(self):
        pass

    def on_update(self, data):
        new_rows = super().get(self.entrypoint_url_temp +
                               '/modification-table-diff?agent_job_id=' +
                               self.last_job_id)
        new_rows = new_rows.json()

        # Checking if the Agent is too outdated and can't be synced
        if not new_rows:
            logger.info('Server Restarting - Automatic Synch not possible')
            self.initialize_graph()
            self.modification_table = super().get(self.entrypoint_url_temp +
                                                  '/modification-table').json()
            if self.modification_table:
                self.last_job_id = self.modification_table[0]['job_id']
            return

        for row in new_rows:
            # falta restart se o resource n tiver na tabela
            if self.graph_operations.get_resource(row['resource_url']):
                if row['method'] == 'POST':
                    self.graph_operations.delete_processing(row['resource_url'])
                    self.get(row['resource_url'])
                elif row['method'] == 'DELETE':
                    self.graph_operations.delete_processing(row['resource_url'])

        # Deleting older rows since they won't be used again
        self.modification_table = new_rows
        self.last_job_id = self.modification_table[0]['job_id']

    def on_broadcast_event(self, data):
        pass
if __name__ == "__main__":
    pass
