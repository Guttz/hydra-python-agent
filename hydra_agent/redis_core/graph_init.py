from redisgraph import Graph, Node
import urllib.request
import json
from hydra_python_core import doc_maker, doc_writer
from graphviz import Digraph
from hydra_agent.redis_core.classes_objects import ClassEndpoints,RequestError
from hydra_agent.redis_core.collections_endpoint import CollectionEndpoints
from hydra_agent.redis_core.redis_proxy import RedisProxy


class InitialGraph:


    def get_apistructure(self,entrypoint_node, api_doc):
        """ It breaks the endpoint into two parts collection and classes"""
        self.collection_endpoints = {}
        self.class_endpoints = {}
        print("split entrypoint into 2 types of endpoints collection and classes")
        for support_property in api_doc.entrypoint.entrypoint.supportedProperty:
            if isinstance(
                    support_property,
                    doc_writer.EntryPointClass):
                self.class_endpoints[support_property.name] = support_property.id_

            if isinstance(
                    support_property,
                    doc_writer.EntryPointCollection):
                self.collection_endpoints[support_property.name] = support_property.id_

        if len(self.class_endpoints.keys())>0:
            clas = ClassEndpoints(self.redis_graph, self.class_endpoints)
            clas.endpointclasses(entrypoint_node, api_doc, self.url)

        if len(self.collection_endpoints.keys())>0:
            coll = CollectionEndpoints(self.redis_graph, self.class_endpoints)
            coll.endpointCollection(
                self.collection_endpoints,
                entrypoint_node,
                api_doc,
                self.url)


    def get_endpoints(self,api_doc, redis_connection):
        """Create node for entrypoint"""
        print("creating entrypoint node")
        entrypoint_properties = {}
        entrypoint_properties["@id"] = str("vocab:Entrypoint")
        entrypoint_properties["url"] = str(
            api_doc.entrypoint.url) + str(api_doc.entrypoint.api)
        entrypoint_properties["supportedOperation"] = "GET"
        entrypoint_node = Node(
            label="id",
            alias="Entrypoint",
            properties=entrypoint_properties)
        self.redis_graph.add_node(entrypoint_node)
        return self.get_apistructure(entrypoint_node, api_doc)



    def main(self,new_url,api_doc,check_commit):
        redis_connection = RedisProxy()
        redis_con = redis_connection.get_connection()
        self.url = new_url
        self.redis_graph = Graph("apigraph", redis_con)
        print("loading... of graph")
        self.get_endpoints(api_doc, redis_con)
        if check_commit:
            print("commiting")
            self.redis_graph.commit()
            # creating whole the graph in redis
            print("done!!!!")
        # uncomment below 2 lines for getting nodes for whole graph
        # for node in self.redis_graph.nodes.values():
        #    print("\n", node.alias)
        # uncomment the below lines for show the graph stored in redis
        # g = Digraph('redis_graph', filename='hydra_graph.gv')
        # using graphviz for visualization of graph stored in redis
        # for edge in self.redis_graph.edges:
        #    g.edge(edge.src_node.alias, edge.dest_node.alias)
        # g.view()
        # see the graph generated by graphviz
