import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Neo4j Configuration
    NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")
    
    # Data paths - point to parent directory containing all folders
    DATA_DIR = os.getenv("DATA_DIR", "C:\\Users\\Tanishka\\OneDrive\\Documents\\graph-business-system\\backend\\dataset")
    
    # Batch sizes for ingestion
    BATCH_SIZE = 500