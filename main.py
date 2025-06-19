import pandas as pd
from neo4j import GraphDatabase
import os
from dotenv import load_dotenv
from create_company_ship_relationship import RelationshipCreator
# 加载环境变量（如果有）
load_dotenv()

class ShippingKnowledgeGraph:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.relationship_creator = RelationshipCreator(uri, user, password)  # 初始化关系创建器
    
    def close(self):
        self.driver.close()
        if hasattr(self, 'relationship_creator'):
            self.relationship_creator.close()
    
    def _execute_query(self, query, parameters=None):
        with self.driver.session() as session:
            return session.run(query, parameters or {}).data()
    
    def clear_database(self):
        """清空数据库，用于重新导入"""
        query = "MATCH (n) DETACH DELETE n"
        self._execute_query(query)
        print("数据库已清空")
    
    def import_companies(self, file_path):
        """导入航运公司数据"""
        df = pd.read_csv(file_path)
        for _, row in df.iterrows():#当你使用 df.iterrows() 时，它返回一个生成器（generator）对象。这个生成器会逐行产生数据，每次迭代返回一个元组 (index, row)
            query = """
            MERGE (c:Company {code: $code})
            SET c.name = $name, c.headquarters = $headquarters
            """
            self._execute_query(query, {
                "code": row['公司注册码'],
                "name": row['公司名称'],
                "headquarters": row['总部所在地']
            })
        print(f"成功导入 {len(df)} 家航运公司")
    
    def import_ships(self, file_path):
        """导入船舶数据并关联公司"""
        df = pd.read_csv(file_path)
        for _, row in df.iterrows():
            # 创建船舶节点
            query = """
            MERGE (s:Ship {imo: $imo})
            SET s.name = $name, s.type = $type, 
                s.speed = $speed, s.power = $power,
                s.gross_tonnage = $gross_tonnage,
                s.dwt = $dwt
            """
            self._execute_query(query, {
                "imo": row['IMO编号'],
                "name": row['船舶名称'],
                "type": row['船舶类型'],
                "speed": row['设计航速(节)'],
                "power": row['主机功率(kW)'],
                "gross_tonnage": row['总吨位'],
                "dwt": row['载重吨位(DWT)']
            })
            
            # 关联公司
            query = """
            MATCH (c:Company {code: $company_id})
            MATCH (s:Ship {imo: $imo})
            MERGE (c)-[:OWNS]->(s)
            """
            self._execute_query(query, {
                "company_id": row['所属公司ID'],
                "imo": row['IMO编号']
            })
        print(f"成功导入 {len(df)} 艘船舶")
    
    def import_ports(self, file_path):
        """导入港口数据"""
        df = pd.read_csv(file_path)
        for _, row in df.iterrows():
            query = """
            MERGE (p:Port {code: $code})
            SET p.name = $name, p.congestion = $congestion,
                p.max_dwt = $max_dwt
            """
            self._execute_query(query, {
                "code": row['五位码'],
                "name": row['港口名称'],
                "congestion": row['拥挤程度(1-10)'],
                "max_dwt": row['最大靠泊能力(DWT)']
            })
        print(f"成功导入 {len(df)} 个港口")
    def import_routes(self, file_path):
        """导入航线数据，使用评分列作为权重（修正循环逻辑）"""
        df = pd.read_csv(file_path)
        print(f"成功读取 {len(df)} 条航线数据")

        # 检查列名（调试用）
        print(f"CSV列名: {list(df.columns)}")
        required_columns = ['起始港口五位码', '目的港口五位码', '航线名称', '航线距离(海里)', '航线天气影响评分(1-10)', '评分']
        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            raise ValueError(f"缺少列: {missing}")

        with self.driver.session() as session:
            for i, row in df.iterrows():
                query = """
                MERGE (from:Port {code: $from_code})
                MERGE (to:Port {code: $to_code})
                MERGE (from)-[r:ROUTE {name: $route_name}]->(to)
                SET r.distance = $distance,
                    r.weather_score = $weather_score,
                    r.rating = toFloat($rating)  
                """
                # 关键：在循环内执行session.run
                session.run(query, {
                    "from_code": row['起始港口五位码'],
                    "to_code": row['目的港口五位码'],
                    "route_name": row['航线名称'],
                    "distance": row['航线距离(海里)'],
                    "weather_score": row['航线天气影响评分(1-10)'],
                    "rating": row['评分']
                })
                # 打印进度（每10条）
                if (i+1) % 10 == 0:
                    print(f"已导入 {i+1}/{len(df)} 条航线")
    
        # 验证导入结果
        with self.driver.session() as session:
            result = session.run("MATCH ()-[r:ROUTE]->() RETURN count(r) as cnt")
            print(f"成功导入 {result.single()['cnt']} 条航线")        
    

    
    def import_ship_port_adaptation(self, file_path):
        """导入船舶与港口的适配关系"""
        df = pd.read_csv(file_path)
        for _, row in df.iterrows():
            query = """
            MATCH (s:Ship {imo: $imo})
            MATCH (p:Port {code: $port_code})
            MERGE (s)-[r:CAN_DOCK]-(p)
            SET r.ship_dwt = $ship_dwt, r.port_max_dwt = $port_max_dwt,
                r.can_dock = $can_dock
            """
            self._execute_query(query, {
                "imo": row['船舶编号'],
                "port_code": row['港口五位码'],
                "ship_dwt": row['船舶载重吨(DWT)'],
                "port_max_dwt": row['港口最大靠泊能力(DWT)'],
                "can_dock": row['是否可停靠'] == "是"
            })
        print(f"成功导入 {len(df)} 条船舶港口适配记录")
    
    def import_ship_port_visits(self, file_path):
        """导入船舶挂靠港口记录"""
        df = pd.read_csv(file_path)
        for _, row in df.iterrows():
            query = """
            MATCH (s:Ship {imo: $imo})
            MATCH (p:Port {code: $port_code})
            MERGE (s)-[r:VISITED {arrival: $arrival, departure: $departure}]-(p)
            SET r.duration = $duration
            """
            self._execute_query(query, {
                "imo": row['船舶编号'],
                "port_code": row['港口五位码'],
                "arrival": row['到达时间'],
                "departure": row['离开时间'],
                "duration": row['停靠时长(小时)']
            })
        print(f"成功导入 {len(df)} 条船舶挂靠记录")
    
    def import_cargo(self, file_path):
        """导入货物数据"""
        df = pd.read_csv(file_path)
        for _, row in df.iterrows():
            query = """
            MERGE (c:Cargo {id: $id})
            SET c.name = $name, c.type = $type, c.weight = $weight
            """
            self._execute_query(query, {
                "id": row['货物编号'],
                "name": row['货物名称'],
                "type": row['货物类型'],
                "weight": row['重量(吨)']
            })
        print(f"成功导入 {len(df)} 条货物记录")
    def create_company_ship_relationships(self, ships_file, companies_file):
            """创建公司与船舶的关系"""
            print("开始创建公司与船舶的关系...")
            success, failure = self.relationship_creator.create_company_ship_relationship(
                ships_file, companies_file
            )
            print(f"成功创建 {success} 条公司-船舶关系，{failure} 条失败")
            return success, failure    
    def find_optimal_route(self, from_port_code, to_port_code):
        """查找最优航线（基于评分列）"""
        query = """
        MATCH (from:Port {code: $from_code}), (to:Port {code: $to_code})
        // 使用评分列作为权重，评分越高表示路径越好
        MATCH path = shortestPath((from)-[r:ROUTE*]->(to))
        UNWIND relationships(path) AS route
        WITH path, 
             sum(route.distance) as total_distance,
             sum(route.weather_score) as total_weather_score,
             sum(route.rating) as total_rating
        RETURN path, total_distance, total_weather_score, total_rating
        ORDER BY total_rating DESC  // 按评分降序排列
        LIMIT 1
        """
        with self.driver.session() as session:
            result = session.run(query, {
                "from_code": from_port_code,
                "to_code": to_port_code
            })
            record = result.single()
            
            if record:
                path = record["path"]
                total_distance = record["total_distance"]
                total_weather_score = record["total_weather_score"]
                total_rating = record["total_rating"]
                
                print(f"找到最优航线：总距离 {total_distance} 海里，总天气评分 {total_weather_score}，总评分 {total_rating:.2f}")
                return {
                    "path": path,
                    "total_distance": total_distance,
                    "total_weather_score": total_weather_score,
                    "total_rating": total_rating
                }
            else:
                print(f"未找到从 {from_port_code} 到 {to_port_code} 的航线")
                return None
    
        if result:
            path = result[0]['path']
            distance = result[0]['total_distance']
            weather_score = result[0]['total_weather_score']
            print(f"找到最优航线：距离 {distance} 海里，天气评分总和 {weather_score}")
            return path
        else:
            print(f"未找到从 {from_port_code} 到 {to_port_code} 的航线")
            return None
    

# 使用示例
if __name__ == "__main__":
    # 替换为你的Neo4j连接信息
    kg = ShippingKnowledgeGraph(
        uri="bolt://localhost:7687",
        user="neo4j",
        password="2003.guo"
    )
    
    try:
        # 清空数据库（可选）
        kg.clear_database()
        
        # 定义数据文件路径（根据你的实际路径调整）
        data_dir = "data"  # 假设data文件夹与脚本同级
        companies_file = os.path.join(data_dir, "航运公司数据.CSV")
        ships_file = os.path.join(data_dir, "船舶信息.CSV")
        ports_file = os.path.join(data_dir, "全球港口信息.CSV")
        routes_file = os.path.join(data_dir, "全球航线数据_rated.CSV")
        cargo_file = os.path.join(data_dir, "全球货物数据.CSV")
        
        relationships_dir = os.path.join(data_dir, "relationships")
        adaption_file = os.path.join(relationships_dir, "船舶港口适配表.CSV")
        visits_file = os.path.join(relationships_dir, "船舶港口挂靠记录.CSV")
        
        # 导入数据
        kg.import_companies(companies_file)
        kg.import_ships(ships_file)
        kg.import_ports(ports_file)
        kg.import_routes(routes_file)
        kg.import_cargo(cargo_file)
        kg.import_ship_port_adaptation(adaption_file)
        kg.import_ship_port_visits(visits_file)
        kg.create_company_ship_relationships(ships_file, companies_file)
        
        # 示例：查找从韶关港到纽约港的最优航线
        optimal_route = kg.find_optimal_route("CNSHA", "USNYC")
        
    finally:
        kg.close()