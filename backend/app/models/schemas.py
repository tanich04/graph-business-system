from pydantic import BaseModel
from typing import List, Optional, Dict, Any

class NodeInfo(BaseModel):
    id: str
    label: str
    properties: Dict[str, Any]

class RelationshipInfo(BaseModel):
    source: str
    target: str
    type: str
    properties: Dict[str, Any]

class GraphSubgraph(BaseModel):
    nodes: List[NodeInfo]
    edges: List[RelationshipInfo]

class NodeMetadata(BaseModel):
    id: str
    type: str
    properties: Dict[str, Any]
    relationships: List[Dict[str, Any]]

class QueryRequest(BaseModel):
    question: str
    session_id: Optional[str] = None

class QueryResponse(BaseModel):
    answer: str
    query_used: Optional[str] = None
    nodes_mentioned: List[str] = []
    data: Optional[List[Dict]] = None
    success: bool = True