# src/database.py
from neo4j import GraphDatabase
from config.settings import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD


class Neo4jConnection:
    def __init__(self):
        self.driver = None

    def connect(self):
        self.driver = GraphDatabase.driver(
            NEO4J_URI,
            auth=(NEO4J_USER, NEO4J_PASSWORD)
        )
        return self.driver

    def close(self):
        if self.driver is not None:
            self.driver.close()

    def execute_query(self, query, parameters=None):
        """执行Cypher查询"""
        if self.driver is None:
            self.connect()

        with self.driver.session() as session:
            result = session.run(query, parameters)
            return result.data()

    def clear_database(self):
        """清空数据库中的所有节点和关系"""
        query = "MATCH (n) DETACH DELETE n"
        self.execute_query(query)