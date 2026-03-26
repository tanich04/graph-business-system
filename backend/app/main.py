from contextlib import asynccontextmanager
import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.exceptions import RequestValidationError
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
import os
import json
from dotenv import load_dotenv
from datetime import datetime
load_dotenv()

from app.database.neo4j_client import neo4j_client
from app.services.graph_service import GraphService
from app.services.llm_service import LLMService  # Fixed import
from app.models.schemas import (
    NodeInfo, GraphSubgraph, NodeMetadata, 
    QueryRequest, QueryResponse
)
from app.middleware.rate_limit import rate_limit_middleware
from app.services.cache_service import cache_service

# Global instances
graph_service = None
llm_service = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for startup and shutdown events
    """
    # Startup
    print("🚀 Starting up...")
    
    # Initialize services
    global graph_service, llm_service
    graph_service = GraphService(neo4j_client)
    llm_service = LLMService()
    
    # Connect to Neo4j
    await neo4j_client.connect()
    print("✅ Connected to Neo4j")
    if cache_service.enabled:
        print("✅ Redis cache enabled")
    else:
        print("⚠️ Redis cache disabled")

    try:
        schema = await graph_service.get_schema()
        print(f"✅ Graph schema loaded: {len(schema.get('node_types', []))} node types found")
    except Exception as e:
        print(f"⚠️ Warning: Could not load schema: {e}")
    
    # Verify LLM service
    if llm_service.use_mock:
        print("⚠️ LLM service running in mock mode (no Groq API key)")
    else:
        print("✅ LLM service initialized with Groq")

    yield  # Application runs here
    
    # Shutdown
    print("🛑 Shutting down...")
    await neo4j_client.close()
    if cache_service.enabled and cache_service.client:
        cache_service.client.close()
    print("✅ Services cleaned up")

# Create FastAPI app with lifespan
app = FastAPI(
    title="Graph Business System API",
    version="1.0.0",
    lifespan=lifespan
)

# Configure CORS
# Update CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:3000",
        "http://127.0.0.1:5173",
        "http://127.0.0.1:3000",
        "http://localhost:8000",
        "*"  # Allow all origins for development (remove in production)
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["*"],
    max_age=3600,
)
app.middleware("http")(rate_limit_middleware)

# ==================== Graph Endpoints ====================

@app.get("/")
async def root():
    return {
        "message": "Graph Business System API",
        "version": "1.0.0",
        "status": "running",
        "endpoints": [
            "/api/graph/nodes",
            "/api/graph/node/{node_id}",
            "/api/graph/neighbors/{node_id}",
            "/api/graph/subgraph",
            "/api/graph/schema",
            "/api/graph/search",
            "/api/chat/query",
            "/api/chat/suggestions"
        ]
    }

@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Check Neo4j connection
        driver = await neo4j_client.connect()
        await driver.verify_connectivity()
        return {
            "status": "healthy",
            "neo4j": "connected",
            "services": {
                "graph_service": graph_service is not None,
                "llm_service": llm_service is not None
            }
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "neo4j": "disconnected"
        }

@app.get("/health")
async def health_check():
    """Detailed health check endpoint"""
    health_status = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "services": {}
    }
    
    # Check Neo4j
    try:
        if neo4j_client.driver:
            await neo4j_client.driver.verify_connectivity()
            health_status["services"]["neo4j"] = "connected"
        else:
            health_status["services"]["neo4j"] = "disconnected"
            health_status["status"] = "degraded"
    except Exception as e:
        health_status["services"]["neo4j"] = f"error: {str(e)}"
        health_status["status"] = "degraded"
    
    # Check Redis cache
    try:
        if cache_service.enabled and cache_service.client:
            cache_service.client.ping()
            health_status["services"]["redis"] = "connected"
        else:
            health_status["services"]["redis"] = "disabled"
    except Exception as e:
        health_status["services"]["redis"] = f"error: {str(e)}"
        health_status["status"] = "degraded"
    
    # Check LLM service - FIXED: use global llm_service
    try:
        global llm_service
        if llm_service:
            if llm_service.use_mock:
                health_status["services"]["llm"] = "mock_mode"
                health_status["status"] = "degraded"  # Still degraded but acceptable
            else:
                health_status["services"]["llm"] = "connected"
        else:
            health_status["services"]["llm"] = "not_initialized"
            health_status["status"] = "degraded"
    except Exception as e:
        health_status["services"]["llm"] = f"error: {str(e)}"
        health_status["status"] = "degraded"
    
    # Check Graph Service
    try:
        if graph_service:
            health_status["services"]["graph"] = "available"
        else:
            health_status["services"]["graph"] = "not_initialized"
            health_status["status"] = "degraded"
    except Exception as e:
        health_status["services"]["graph"] = f"error: {str(e)}"
        health_status["status"] = "degraded"
    
    # Overall status - only healthy if all critical services are connected
    critical_services = ["neo4j", "graph"]
    all_critical_ok = all(
        health_status["services"].get(svc) == "connected" or 
        health_status["services"].get(svc) == "available"
        for svc in critical_services
    )
    
    if all_critical_ok:
        health_status["status"] = "healthy"
    else:
        health_status["status"] = "degraded"
    
    return health_status

@app.get("/api/graph/nodes", response_model=List[NodeInfo])
async def get_nodes(
    node_type: Optional[str] = Query(None, description="Filter by node type/label"),
    limit: int = Query(50, ge=1, le=200),
    skip: int = Query(0, ge=0)
):
    """
    Get nodes with optional type filtering and pagination
    """
    try:
        nodes = await graph_service.get_nodes(node_type, limit, skip)
        return nodes
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/graph/node/{node_id}", response_model=NodeMetadata)
async def get_node_metadata(node_id: str):
    """
    Get detailed metadata for a specific node
    """
    try:
        metadata = await graph_service.get_node_metadata(node_id)
        if not metadata:
            raise HTTPException(status_code=404, detail=f"Node {node_id} not found")
        return metadata
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/graph/neighbors/{node_id}")
async def get_neighbors(
    node_id: str,
    depth: int = Query(1, ge=1, le=3),
    limit: int = Query(50, ge=1, le=200)
):
    """
    Get neighbors of a node up to specified depth
    """
    try:
        subgraph = await graph_service.get_neighbors(node_id, depth, limit)
        return subgraph
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/graph/node-relationships/{node_type}")
async def get_node_relationships_by_type(
    node_type: str,
    limit: int = Query(20, ge=1, le=100)
):
    """
    Get relationships for a specific node type (for summary nodes)
    """
    try:
        relationships = await graph_service.get_node_relationships_by_type(node_type, limit)
        return relationships
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/api/graph/subgraph")
async def get_subgraph(node_ids: List[str]):  # This expects a list directly
    """
    Get subgraph containing specified nodes and their connections
    """
    try:
        subgraph = await graph_service.get_subgraph(node_ids)
        return subgraph
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/graph/schema")
async def get_schema():
    """
    Get the graph schema (node types and relationship types)
    """
    try:
        schema = await graph_service.get_schema()
        return schema
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/graph/search")
async def search_nodes(
    query: str = Query(..., min_length=1),
    node_type: Optional[str] = None,
    limit: int = Query(20, ge=1, le=100)
):
    """
    Search for nodes by property values
    """
    try:
        results = await graph_service.search_nodes(query, node_type, limit)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/graph/statistics")
async def get_statistics():
    """
    Get high-level statistics about the graph.
    """
    try:
        # Get node counts
        node_counts = await graph_service.get_node_counts()
        
        # Get relationship counts using graph_service to avoid connection issues
        relationship_counts = await graph_service.get_relationship_counts()
        
        return {
            "total_nodes": sum(node_counts.values()),
            "node_counts": node_counts,
            "relationship_counts": relationship_counts,
            "total_relationships": sum(relationship_counts.values())
        }
    except Exception as e:
        print(f"Error in statistics endpoint: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch statistics: {str(e)}")

# ==================== Chat/Query Endpoints ====================

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    return JSONResponse(
        status_code=422,
        content={
            "error": "Validation error",
            "details": exc.errors(),
            "message": "Invalid request parameters"
        }
    )

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "status_code": exc.status_code
        }
    )

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    import traceback
    print(f"Unhandled error: {traceback.format_exc()}")
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "message": "An unexpected error occurred. Please try again later."
        }
    )

@app.post("/api/chat/query", response_model=QueryResponse)
async def chat_query(request: QueryRequest):
    """
    Process natural language query and return data-backed response
    """
    try:
        # Validate query is on-topic
        is_valid, error_message = llm_service.validate_query(request.question)
        if not is_valid:
            return QueryResponse(
                answer=error_message,
                query_used=None,
                nodes_mentioned=[],
                success=False
            )
        
        # Generate Cypher query from natural language
        cypher_query = await llm_service.generate_cypher(request.question)
        
        # Execute query
        result = await graph_service.execute_cypher(cypher_query)
        
        # Generate natural language response
        answer = await llm_service.generate_response(request.question, result)
        
        # Extract node IDs mentioned in result for highlighting
        nodes_mentioned = graph_service.extract_node_ids(result)
        
        return QueryResponse(
            answer=answer,
            query_used=cypher_query,
            nodes_mentioned=nodes_mentioned,
            data=result[:10] if result else [],  # Limit data in response
            success=True
        )
    except Exception as e:
        return QueryResponse(
            answer=f"Error processing query: {str(e)}",
            query_used=None,
            nodes_mentioned=[],
            success=False
        )

@app.post("/api/chat/query-stream")
async def chat_query_stream(request: QueryRequest):
    """
    Process natural language query with streaming response
    """
    try:
        # Validate query
        is_valid, error_message = llm_service.validate_query(request.question)
        if not is_valid:
            return StreamingResponse(
                iter([json.dumps({"type": "error", "content": error_message})]),
                media_type="text/event-stream"
            )
        
        async def generate():
            # Step 1: Generate Cypher query
            yield json.dumps({"type": "query_start", "content": "Generating query..."}) + "\n"
            
            cypher_parts = []
            async for chunk in llm_service.generate_cypher_stream(request.question):
                cypher_parts.append(chunk)
                yield json.dumps({"type": "query_chunk", "content": chunk}) + "\n"
            
            cypher_query = ''.join(cypher_parts)
            
            # Step 2: Execute query
            yield json.dumps({"type": "executing", "content": "Executing query..."}) + "\n"
            
            try:
                result = await graph_service.execute_cypher(cypher_query)
                nodes_mentioned = graph_service.extract_node_ids(result)
                
                yield json.dumps({"type": "nodes", "content": nodes_mentioned}) + "\n"
                yield json.dumps({"type": "data", "content": result[:50]}) + "\n"
                
                # Step 3: Generate response
                yield json.dumps({"type": "response_start", "content": "Generating response..."}) + "\n"
                
                async for chunk in llm_service.generate_response_stream(request.question, result):
                    yield json.dumps({"type": "response_chunk", "content": chunk}) + "\n"
                
                yield json.dumps({"type": "done", "content": ""}) + "\n"
                
            except Exception as e:
                yield json.dumps({"type": "error", "content": f"Query execution failed: {str(e)}"}) + "\n"
        
        return StreamingResponse(generate(), media_type="text/event-stream")
        
    except Exception as e:
        return StreamingResponse(
            iter([json.dumps({"type": "error", "content": str(e)})]),
            media_type="text/event-stream"
        )
    
@app.post("/api/chat/stream")
async def chat_stream(request: QueryRequest):
    """Stream chat responses"""
    async def generate():
        # Validate query
        is_valid, error_message = llm_service.validate_query(request.question)
        if not is_valid:
            yield f"data: {json.dumps({'error': error_message})}\n\n"
            return
        
        # Generate and stream Cypher
        yield f"data: {json.dumps({'type': 'query', 'content': 'Generating query...'})}\n\n"
        cypher_query = await llm_service.generate_cypher(request.question)
        yield f"data: {json.dumps({'type': 'query', 'content': cypher_query})}\n\n"
        
        # Execute query
        yield f"data: {json.dumps({'type': 'executing', 'content': 'Executing query...'})}\n\n"
        result = await graph_service.execute_cypher(cypher_query)
        
        # Stream response
        yield f"data: {json.dumps({'type': 'results', 'count': len(result)})}\n\n"
        
        # Generate and stream natural language response
        answer = await llm_service.generate_response(request.question, result)
        
        # Stream answer in chunks
        for chunk in answer.split(' '):
            yield f"data: {json.dumps({'type': 'answer', 'content': chunk + ' '})}\n\n"
            await asyncio.sleep(0.05)
        
        yield f"data: {json.dumps({'type': 'done'})}\n\n"
    
    return StreamingResponse(generate(), media_type="text/event-stream")

@app.get("/api/chat/suggestions")
async def get_suggested_queries():
    """
    Get suggested queries for users
    """
    return {
        "suggestions": [
            "Which products are associated with the highest number of billing documents?",
            "Show me the full flow for billing document 90504248",
            "Find sales orders that have been delivered but not billed",
            "Which customers have the highest total order value?",
            "Show me all incomplete orders",
            "What are the top 5 products by sales volume?",
            "Trace the payment status for invoice 90504219",
            "Find orders with delivery issues",
            "How many billing documents does product B8907367041603 have?",
            "Show me all deliveries from shipping point 1920",
            "Which storage locations hold product 3001456?",
            "Which company code is billing document 90504248 in?",
            "Show the sales area assignments for customer 310000108"
        ]
    }


@app.get("/health")
async def health_check():
    """Detailed health check endpoint"""
    health_status = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "services": {}
    }
    
    # Check Neo4j
    try:
        if neo4j_client.driver:
            await neo4j_client.driver.verify_connectivity()
            health_status["services"]["neo4j"] = "connected"
        else:
            health_status["services"]["neo4j"] = "disconnected"
            health_status["status"] = "degraded"
    except Exception as e:
        health_status["services"]["neo4j"] = f"error: {str(e)}"
        health_status["status"] = "degraded"
    
    # Check Redis cache
    try:
        from app.services.cache_service import cache_service
        if cache_service.enabled and cache_service.client:
            cache_service.client.ping()
            health_status["services"]["redis"] = "connected"
        else:
            health_status["services"]["redis"] = "disabled"
    except Exception as e:
        health_status["services"]["redis"] = f"error: {str(e)}"
    
    # Check LLM service
    try:
        from app.services.llm_service import llm_service
        if llm_service and not llm_service.use_mock:
            health_status["services"]["llm"] = "connected"
        else:
            health_status["services"]["llm"] = "mock_mode"
    except Exception as e:
        health_status["services"]["llm"] = f"error: {str(e)}"
    
    # Overall status
    if all(s == "connected" for s in health_status["services"].values()):
        health_status["status"] = "healthy"
    elif any("error" in str(s) for s in health_status["services"].values()):
        health_status["status"] = "unhealthy"
    else:
        health_status["status"] = "degraded"
    
    return health_status
    
@app.get("/api/graph/overview")
async def get_graph_overview():
    """
    Get an aggregated overview of the graph for initial visualization.
    Returns summary nodes with counts instead of all individual nodes.
    """
    try:
        overview = await graph_service.get_graph_overview()
        return overview
    except Exception as e:
        print(f"Error getting graph overview: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/graph/nodes/paginated")
async def get_nodes_paginated(
    node_type: Optional[str] = Query(None, description="Filter by node type/label"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    sort_by: Optional[str] = Query(None, description="Sort by property"),
    sort_order: str = Query("asc", description="Sort order (asc/desc)")
):
    """
    Get paginated nodes for efficient loading.
    """
    try:
        skip = (page - 1) * page_size
        nodes = await graph_service.get_nodes(node_type, page_size, skip, sort_by, sort_order)
        
        # Get total count for pagination
        total_count = await graph_service.get_node_count(node_type)
        
        return {
            "nodes": nodes,
            "page": page,
            "page_size": page_size,
            "total_count": total_count,
            "total_pages": (total_count + page_size - 1) // page_size
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
     
@app.post("/api/chat/validate")
async def validate_question(question: QueryRequest):
    """
    Validate if a question is relevant to the business domain
    """
    is_valid, message = llm_service.validate_query(question.question)
    return {
        "valid": is_valid,
        "message": message,
        "question": question.question
    }

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    host = os.getenv("HOST", "0.0.0.0")
    
    print(f"🚀 Starting Graph Business System API")
    print(f"📍 Host: {host}")
    print(f"🔌 Port: {port}")
    print(f"📊 Neo4j: {os.getenv('NEO4J_URI', 'bolt://localhost:7687')}")
    
    uvicorn.run(
        "app.main:app",
        host=host,
        port=port,
        reload=True,
        log_level="info"
    )
