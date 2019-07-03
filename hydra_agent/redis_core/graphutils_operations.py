import urllib.request
import json
import logging
from urllib.error import URLError, HTTPError
from hydra_agent.redis_core.redis_proxy import RedisProxy
from hydra_agent.redis_core.graphutils import GraphUtils
from redisgraph import Graph, Node
from requests import Session

logger = logging.getLogger(__file__)


class GraphOperations():

    def __init__(self, entrypoint_url: str, api_doc: dict,
                 redis_proxy: RedisProxy):
        self.entrypoint_url = entrypoint_url
        self.api_doc = api_doc
        self.redis_proxy = redis_proxy
        self.redis_connection = redis_proxy.get_connection()
        self.vocabulary = 'vocab'
        self.graph_utils = GraphUtils(redis_proxy)
        self.redis_graph = Graph("apigraph", self.redis_connection)
        self.session = Session()

    def get_processing(self, url: str, resource: dict) -> None:
        """Synchronize Redis upon new GET operations
        :param url: Resource URL to be updated in Redis.
        :param resource: Resource object fetched from server.
        :return: None.
        """
        url_list = url.rstrip('/').replace(self.entrypoint_url, "EntryPoint")
        url_list = url_list.split('/')
        # Updating Redis
        # First case - When processing a GET for a resource
        if len(url_list) == 3:
            entrypoint, resource_endpoint, resource_id = url_list

            # Building the the collection id, i.e. vocab:Entrypoint/Collection
            redis_collection_id = self.vocabulary + \
                ":" + entrypoint + \
                "/" + resource_endpoint

            collection_members = self.graph_utils.read(
                match=":collection",
                where="id='{}'".format(redis_collection_id),
                ret=".members")

            # Checking if it's the first member to be loaded
            if collection_members is None:
                collection_members = []
            else:
                collection_members = eval(collection_members[0]['members'])

            collection_members.append({'@id': resource['@id'],
                                       '@type': resource['@type']})
            # Updating the collection properties with the nem member
            self.graph_utils.update(
                match="collection",
                where="id='{}'".format(redis_collection_id),
                set="members = \"{}\"".format(str(collection_members)))

            # Creating node for new collection member and commiting to Redis
            self.graph_utils.add_node("objects" + resource['@type'],
                                      resource['@type'] + resource_id,
                                      resource)
            # Commits the graph
            self.graph_utils.flush()

            # Creating relation between collection node and member
            self.graph_utils.create_relation(label_source="collection",
                                             where_source="type : \'" +
                                             resource_endpoint + "\'",
                                             relation_type="has_" +
                                             resource['@type'],
                                             label_dest="objects" +
                                             resource['@type'],
                                             where_dest="id : \'" +
                                             resource['@id'] + "\'")

            # Checking for embedded resources in the properties of resource
            class_doc = self.api_doc.parsed_classes[resource['@type']]['class']
            supported_properties = class_doc.supportedProperty
            for supported_prop in supported_properties:
                    if (self.vocabulary + ":") in str(supported_prop.prop):
                        if resource[supported_prop.title]:
                            discovered_url = self.entrypoint_url.replace(
                                self.api_doc.entrypoint.api, "").rstrip("/")
                            discovered_url = discovered_url + \
                                resource[supported_prop.title]                   
                            self.embedded_resource(resource['@id'],
                                                   resource['@type'],
                                                   discovered_url)

            return
        # Second Case - When processing a GET for a Collection
        elif len(url_list) == 2:
            entrypoint, resource_endpoint = url_list
            redis_collection_id = self.vocabulary + \
                ":" + entrypoint + \
                "/" + resource_endpoint

            self.graph_utils.update(
                match="collection",
                where="id='{}'".format(redis_collection_id),
                set="members = \"{}\"".format(str(resource["members"])))
            return

        # Third Case - When processing a valid GET that is not compatible-
        # with the Redis Hydra structure, only returns response
        else:
            logger.info("No modification to Redis was made")
            return

    def put_processing(self, url: str, new_object: dict) -> None:
        """Synchronize Redis upon new PUT operations
        :param url: URL for the resource to be created.
        :return: None.
        """
        # Manually add the id that will be on the server for the object added
        url_list = url.split('/', 3)
        new_object["@id"] = '/' + url_list[-1]
        # Simply call sync_get to add the resource to the collection at Redis
        self.get_processing(url, new_object)
        return

    def post_processing(self, url: str, updated_object: dict) -> None:
        """Synchronize Redis upon new POST operations
        :param url: URL for the resource to be updated.
        :return: None.
        """
        # Manually add the id that will be on the server for the object added
        url_list = url.split('/', 3)
        updated_object["@id"] = '/' + url_list[-1]

        # Simply call sync_get to add the resource to the collection at Redis
        self.delete_processing(url)
        self.get_processing(url, updated_object)
        return

    def delete_processing(self, url: str) -> None:
        """Synchronize Redis upon new DELETE operations
        :param url: URL for the resource deleted.
        :return: None.
        """
        # MEMBER NODE Deleting from Redis Graph
        url_list = url.split('/', 3)
        object_id = '/' + url_list[-1]

        self.graph_utils.delete(where="id='{}'".format(object_id))

        # COLLECTION Property members update
        url = url.rstrip('/').replace(self.entrypoint_url, "EntryPoint")
        entrypoint, resource_endpoint, resource_id = url.split('/')

        # Building the the collection id, i.e. vocab:Entrypoint/Collection
        redis_collection_id = self.vocabulary + \
            ":" + entrypoint + \
            "/" + resource_endpoint

        collection_members = self.graph_utils.read(
            match=":collection",
            where="id='{}'".format(redis_collection_id),
            ret=".members")

        # Checking if it's the first member to be loaded
        if collection_members is None:
            return
        else:
            collection_members = eval(collection_members[0]['members'])

        for member in collection_members:
            if resource_id in member['@id']:
                collection_members.remove(member)

        self.graph_utils.update(
            match="collection",
            where="id='{}'".format(redis_collection_id),
            set="members = \"{}\"".format(str(collection_members)))

        return

    def get_resource(self, url: str) -> dict:
        """Get resources already stored on Redis and return
        :param url: URL for the resource to fetch.
        :return: Object with resource found.
        """
        # This is the first step to interact with Redis properly
        # This method should eventually accept a type, a id or an url
        # do the proper checking and then return the cached info
        url_aux = url.rstrip('/').replace(self.entrypoint_url, "EntryPoint")
        url_list = url_aux.split('/')

        # Checking if querying for cached Collection or Member
        if len(url_list) == 2:
            entrypoint, resource_endpoint = url_aux.split('/')
            object_id = self.vocabulary + \
                ":" + entrypoint + \
                "/" + resource_endpoint
        else:
            url_list = url.split('/', 3)
            object_id = '/' + url_list[-1]

        resource = self.graph_utils.read(
                            match="",
                            where="id='{}'".format(object_id),
                            ret="")
        # If having only one object/querying by id return only dict
        if resource is not None and len(resource) == 1:
            return resource[0]

        return resource

    def embedded_resource(self, parent_id: str, parent_type: str,
                          discovered_url: str) -> str:
        """Checks for existance of discovered resource and creates links
        for embedded resources inside other resources properties
        :parent_id: Resource ID for the parent node that had this reference
        :parent_type: Resource Type for the parent node that had this reference
        :discovered_url: URL Reference for resource found inside a property
        """
        resource = self.get_resource(discovered_url)
        if resource is None:
            response = self.session.get(discovered_url)
            if response.status_code == 200:
                resource = response.json()
                self.get_processing(discovered_url, resource)
            else:
                logger.info("Embedded link for resource cannot be fetched")
                return

        # Creating relation between collection node and member
        response = self.graph_utils.create_relation(label_source="objects" +
                                                    parent_type,
                                                    where_source="id : \'" +
                                                    parent_id + "\'",
                                                    relation_type="has_" +
                                                    resource['@type'],
                                                    label_dest="objects" +
                                                    resource['@type'],
                                                    where_dest="id : \'" +
                                                    resource['@id'] + "\'")
        return str(response)

if __name__ == "__main__":
    pass
