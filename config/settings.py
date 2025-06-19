# config/settings.py
import os

# Neo4j配置
NEO4J_URI = os.getenv("NEO4J_URI", "0.0.0.0:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "2003.guo")  # 替换为你的密码