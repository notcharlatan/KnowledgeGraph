from neo4j import GraphDatabase
import pandas as pd
import os

class RelationshipCreator:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def close(self):
        self.driver.close()
    
    def create_company_ship_relationship(self, ships_file, companies_file):
        """创建公司与船舶的关系"""
        # 读取船舶和公司数据
        ships_df = pd.read_csv(ships_file)
        companies_df = pd.read_csv(companies_file)
        
        # 检查数据
        if '所属公司ID' not in ships_df.columns:
            raise ValueError("船舶数据中缺少'所属公司ID'列")
        if '公司注册码' not in companies_df.columns:
            raise ValueError("公司数据中缺少'公司注册码'列")
        
        # 获取所有公司注册码
        company_codes = set(companies_df['公司注册码'])
        print(f"已加载 {len(company_codes)} 家公司的注册码")
        
        # 建立关系
        with self.driver.session() as session:
            # 统计成功和失败的数量
            success_count = 0
            failure_count = 0
            
            for _, ship in ships_df.iterrows():
                ship_imo = ship['IMO编号']
                company_id = ship['所属公司ID']
                
                # 检查公司ID是否存在
                if company_id not in company_codes:
                    print(f"警告：公司ID {company_id} 不存在，无法为船舶 {ship_imo} 建立关系")
                    failure_count += 1
                    continue
                
                # 创建关系
                query = """
                MATCH (c:Company {code: $company_id})
                MATCH (s:Ship {imo: $ship_imo})
                MERGE (c)-[:OWNS]->(s)
                """
                try:
                    session.run(query, {
                        "company_id": company_id,
                        "ship_imo": ship_imo
                    })
                    success_count += 1
                except Exception as e:
                    print(f"为船舶 {ship_imo} 建立关系时出错: {str(e)}")
                    failure_count += 1
            
            print(f"关系建立完成: 成功 {success_count} 条, 失败 {failure_count} 条")
            return success_count, failure_count


# 使用示例
if __name__ == "__main__":
    # 替换为您的Neo4j连接信息和文件路径
    creator = RelationshipCreator(
        uri="bolt://localhost:7687",
        user="neo4j",
        password="2003.guo"
    )
    
    try:
        ships_file = r"PythonProject\data\船舶信息.CSV"
        companies_file = r"PythonProject\data\航运公司数据.CSV"
        
        success, failure = creator.create_company_ship_relationship(
            ships_file, companies_file
        )
        
    finally:
        creator.close()