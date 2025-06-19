# src/import_data.py
from typing import List
from .database import Neo4jConnection
from .models import Company, Ship


class DataImporter:
    def __init__(self, connection: Neo4jConnection):
        self.connection = connection

    def import_companies(self, companies: List[Company]):
        """导入公司数据到Neo4j"""
        for company in companies:
            query = """
            MERGE (c:Company {companyName: $companyName})
            SET c.establishYear = $establishYear,
                c.headquarter = $headquarter,
                c.companyType = $companyType,
                c.fleetSize = $fleetSize
            RETURN c
            """
            self.connection.execute_query(query, company.to_dict())

    def import_ships(self, ships: List[Ship]):
        """导入船舶数据到Neo4j"""
        for ship in ships:
            # 创建船舶节点
            query = """
            MERGE (s:Ship {shipName: $shipName})
            SET s.shipType = $shipType,
                s.deadweight = $deadweight,
                s.length = $length,
                s.speed = $speed,
                s.buildYear = $buildYear,
                s.draft = $draft
            RETURN s
            """
            self.connection.execute_query(query, ship.to_dict())

            # 建立公司与船舶的关系
            relation_query = """
            MATCH (c:Company {companyName: $company_name}), 
                  (s:Ship {shipName: $ship_name})
            MERGE (c)-[:OWNS]->(s)
            """
            params = {
                "company_name": ship.company_name,
                "ship_name": ship.ship_name
            }
            self.connection.execute_query(relation_query, params)