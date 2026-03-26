from neo4j import AsyncGraphDatabase
import os
from dotenv import load_dotenv

load_dotenv()

class Neo4jClient:
    def __init__(self):
        self.uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.user = os.getenv("NEO4J_USER", "neo4j")
        self.password = os.getenv("NEO4J_PASSWORD", "password123")
        self.driver = None
        self._connected = False
    
    async def connect(self):
        """Create async connection to Neo4j"""
        try:
            if not self.driver:
                self.driver = AsyncGraphDatabase.driver(
                    self.uri, 
                    auth=(self.user, self.password)
                )
                # Verify connection
                await self.driver.verify_connectivity()
                self._connected = True
                print(f"✅ Connected to Neo4j at {self.uri}")
            return self.driver
        except Exception as e:
            print(f"❌ Failed to connect to Neo4j: {e}")
            self.driver = None
            self._connected = False
            raise
    
    async def close(self):
        """Close the connection"""
        if self.driver:
            await self.driver.close()
            self.driver = None
            self._connected = False
            print("✅ Closed Neo4j connection")
    
    def is_connected(self):
        """Check if connected to Neo4j"""
        return self._connected and self.driver is not None
    
    async def ensure_connection(self):
        """Ensure we have a valid connection"""
        if not self.is_connected():
            await self.connect()
        return self.driver

neo4j_client = Neo4jClient()