from typing import List, Optional, Dict, Any
import re
from app.services.cache_service import cache_service
class GraphService:
    def __init__(self, neo4j_client):
        self.client = neo4j_client
        self._driver = None
    
    async def _get_driver(self):
        """Get the Neo4j driver, ensuring connection"""
        try:
            if not self.client.driver:
                self._driver = await self.client.connect()
            else:
                self._driver = self.client.driver
            return self._driver
        except Exception as e:
            print(f"Error getting driver: {e}")
            # Try to reconnect
            self._driver = await self.client.connect()
            return self._driver
    
    async def get_node_counts(self) -> Dict[str, int]:
        """Get count of nodes by type"""
        try:
            driver = await self._get_driver()
            
            query = """
            MATCH (n)
            RETURN labels(n)[0] as type, count(*) as count
            ORDER BY count DESC
            """
            
            async with driver.session() as session:
                result = await session.run(query)
                records = await result.data()
                return {record['type']: record['count'] for record in records}
        except Exception as e:
            print(f"Error getting node counts: {e}")
            return {}
    
    async def get_relationship_counts(self) -> Dict[str, int]:
        """Get count of relationships by type"""
        try:
            driver = await self._get_driver()
            
            query = """
            MATCH ()-[r]->()
            RETURN type(r) as relationship_type, count(*) as count
            ORDER BY count DESC
            """
            
            async with driver.session() as session:
                result = await session.run(query)
                records = await result.data()
                return {record['relationship_type']: record['count'] for record in records}
        except Exception as e:
            print(f"Error getting relationship counts: {e}")
            return {}
    
    async def get_nodes(self, node_type: Optional[str], limit: int, skip: int, 
                    sort_by: Optional[str] = None, sort_order: str = "asc") -> List[Dict]:
        """Get nodes with filtering, pagination, and sorting"""
        try:
            driver = await self._get_driver()
            
            # Build sort clause
            sort_clause = ""
            if sort_by:
                sort_clause = f"ORDER BY n.`{sort_by}` {sort_order.upper()}"
            
            if node_type:
                query = f"""
                MATCH (n:{node_type})
                RETURN n
                {sort_clause}
                SKIP $skip
                LIMIT $limit
                """
            else:
                # When no node_type specified, get a sample of all nodes
                query = f"""
                MATCH (n)
                RETURN n
                {sort_clause}
                SKIP $skip
                LIMIT $limit
                """
            
            async with driver.session() as session:
                result = await session.run(query, skip=skip, limit=limit)
                records = await result.data()
                
                nodes = []
                for record in records:
                    node = record['n']
                    # Extract ID from node - try different possible ID fields
                    node_id = None
                    for id_field in ['salesOrder', 'billingDocument', 'deliveryDocument', 
                                    'product', 'businessPartner', 'accountingDocument']:
                        if id_field in node:
                            node_id = node[id_field]
                            break
                    
                    if not node_id:
                        # If no standard ID field, use node ID from Neo4j
                        node_id = str(node.element_id)
                    
                    node_type_val = list(node.labels)[0] if node.labels else "Unknown"
                    
                    # Only include nodes with valid IDs
                    if node_id:
                        nodes.append({
                            "id": str(node_id),
                            "label": node_type_val,
                            "properties": {k: str(v)[:200] for k, v in dict(node).items() if v is not None}
                        })
                
                print(f"Found {len(nodes)} nodes for type {node_type or 'all'}")
                return nodes
        except Exception as e:
            print(f"Error getting nodes: {e}")
            return []
        
    async def get_nodes_paginated(self, node_type: Optional[str], page: int, page_size: int) -> Dict:
        """Get paginated nodes for progressive loading"""
        try:
            driver = await self._get_driver()
            skip = (page - 1) * page_size
            
            if node_type:
                query = f"""
                MATCH (n:{node_type})
                RETURN n
                SKIP $skip
                LIMIT $page_size
                """
            else:
                query = """
                MATCH (n)
                RETURN n
                SKIP $skip
                LIMIT $page_size
                """
            
            async with driver.session() as session:
                result = await session.run(query, skip=skip, page_size=page_size)
                records = await result.data()
                
                nodes = []
                for record in records:
                    node = record['n']
                    node_id = (node.get('salesOrder') or node.get('billingDocument') or 
                            node.get('deliveryDocument') or node.get('product') or 
                            node.get('businessPartner') or node.get('accountingDocument'))
                    
                    if node_id:
                        nodes.append({
                            "id": str(node_id),
                            "label": list(node.labels)[0] if node.labels else "Unknown",
                            "properties": {k: str(v)[:200] for k, v in dict(node).items() if v is not None}
                        })
                
                # Get total count
                if node_type:
                    count_query = f"MATCH (n:{node_type}) RETURN count(n) as total"
                else:
                    count_query = "MATCH (n) RETURN count(n) as total"
                
                count_result = await session.run(count_query)
                count_record = await count_result.single()
                total_count = count_record['total'] if count_record else 0
                
                return {
                    "nodes": nodes,
                    "total_count": total_count,
                    "page": page,
                    "page_size": page_size,
                    "total_pages": (total_count + page_size - 1) // page_size
                }
        except Exception as e:
            print(f"Error getting paginated nodes: {e}")
            return {"nodes": [], "total_count": 0, "page": page, "page_size": page_size, "total_pages": 0}
    
    async def get_node_relationships_by_type(self, node_type: str, limit: int = 20) -> List[Dict]:
        """Get sample relationships for a node type"""
        try:
            driver = await self._get_driver()
            
            query = f"""
            MATCH (n:{node_type})-[r]-(connected)
            RETURN n.{self._get_id_field(node_type)} as node_id,
                type(r) as relationship_type,
                labels(connected)[0] as connected_type,
                connected.salesOrder as salesOrder,
                connected.billingDocument as billingDocument,
                connected.deliveryDocument as deliveryDocument,
                connected.product as product,
                connected.businessPartner as customer
            LIMIT $limit
            """
            
            async with driver.session() as session:
                result = await session.run(query, limit=limit)
                records = await result.data()
                return records
        except Exception as e:
            print(f"Error getting relationships by type: {e}")
            return []
    
    def _get_id_field(self, node_type: str) -> str:
        """Get the ID field name for a node type"""
        id_fields = {
            'SalesOrder': 'salesOrder',
            'BillingDocument': 'billingDocument',
            'Delivery': 'deliveryDocument',
            'Product': 'product',
            'Customer': 'businessPartner',
            'JournalEntry': 'accountingDocument',
            'Payment': 'accountingDocument'
        }
        return id_fields.get(node_type, 'id')
    
    async def get_node_count(self, node_type: Optional[str]) -> int:
        """Get total count of nodes (with optional type filter)"""
        try:
            driver = await self._get_driver()
            
            if node_type:
                query = f"MATCH (n:{node_type}) RETURN count(n) as count"
            else:
                query = "MATCH (n) RETURN count(n) as count"
            
            async with driver.session() as session:
                result = await session.run(query)
                record = await result.single()
                return record['count'] if record else 0
        except Exception as e:
            print(f"Error getting node count: {e}")
            return 0
    
    async def get_node_metadata(self, node_id: str) -> Optional[Dict]:
        """Get detailed metadata for a specific node"""
        try:
            driver = await self._get_driver()
            
            # Try multiple ways to find the node
            queries = [
                # Try as salesOrder
                """
                MATCH (n {salesOrder: $node_id})
                RETURN n
                """,
                # Try as billingDocument
                """
                MATCH (n {billingDocument: $node_id})
                RETURN n
                """,
                # Try as deliveryDocument
                """
                MATCH (n {deliveryDocument: $node_id})
                RETURN n
                """,
                # Try as product
                """
                MATCH (n {product: $node_id})
                RETURN n
                """,
                # Try as businessPartner
                """
                MATCH (n {businessPartner: $node_id})
                RETURN n
                """,
                # Try as accountingDocument
                """
                MATCH (n {accountingDocument: $node_id})
                RETURN n
                """
            ]
            
            async with driver.session() as session:
                for query in queries:
                    result = await session.run(query, node_id=node_id)
                    record = await result.single()
                    if record:
                        node = record['n']
                        node_type = list(node.labels)[0] if node.labels else "Unknown"
                        
                        return {
                            "id": node_id,
                            "type": node_type,
                            "properties": dict(node),
                            "relationships": await self.get_node_relationships(node_id)
                        }
                
                # If not found by ID, try partial match
                fallback_query = """
                MATCH (n)
                WHERE toString(n.salesOrder) CONTAINS $node_id
                OR toString(n.billingDocument) CONTAINS $node_id
                OR toString(n.deliveryDocument) CONTAINS $node_id
                OR toString(n.product) CONTAINS $node_id
                RETURN n
                LIMIT 1
                """
                result = await session.run(fallback_query, node_id=node_id)
                record = await result.single()
                if record:
                    node = record['n']
                    node_type = list(node.labels)[0] if node.labels else "Unknown"
                    return {
                        "id": node_id,
                        "type": node_type,
                        "properties": dict(node),
                        "relationships": []
                    }
            
            return None
        except Exception as e:
            print(f"Error getting node metadata: {e}")
            return None
    
    async def get_node_relationships(self, node_id: str) -> List[Dict]:
        """Get all relationships for a node"""
        try:
            driver = await self._get_driver()
            
            query = """
            MATCH (n)-[r]-(connected)
            WHERE n.salesOrder = $node_id 
               OR n.billingDocument = $node_id 
               OR n.deliveryDocument = $node_id
               OR n.product = $node_id
               OR n.businessPartner = $node_id
               OR n.accountingDocument = $node_id
            RETURN type(r) as relationship_type, 
                   labels(connected)[0] as connected_type,
                   connected.salesOrder as salesOrder,
                   connected.billingDocument as billingDocument,
                   connected.deliveryDocument as deliveryDocument,
                   connected.product as product,
                   connected.businessPartner as customer
            LIMIT 50
            """
            
            async with driver.session() as session:
                result = await session.run(query, node_id=node_id)
                records = await result.data()
                return records
        except Exception as e:
            print(f"Error getting node relationships: {e}")
            return []
    
    async def get_neighbors(self, node_id: str, depth: int, limit: int) -> Dict:
        """Get subgraph of neighbors up to specified depth with caching"""
        
        # Check cache first
        cached = await cache_service.get_cached_graph(node_id, depth)
        if cached:
            print(f"Cache hit for {node_id} at depth {depth}")
            return cached
        
        try:
            driver = await self._get_driver()
            
            query = f"""
            MATCH path = (start)-[*1..{depth}]-(neighbor)
            WHERE start.salesOrder = $node_id 
            OR start.billingDocument = $node_id 
            OR start.deliveryDocument = $node_id
            OR start.product = $node_id
            OR start.businessPartner = $node_id
            OR start.accountingDocument = $node_id
            RETURN path
            LIMIT $limit
            """
            
            async with driver.session() as session:
                result = await session.run(query, node_id=node_id, limit=limit)
                records = await result.data()
                
                # Process results (same as before)
                nodes = {}
                edges = []
                
                for record in records:
                    if 'path' in record:
                        path = record['path']
                        for node in path.nodes:
                            node_id_val = node.get('salesOrder') or node.get('billingDocument') or \
                                        node.get('deliveryDocument') or node.get('product') or \
                                        node.get('businessPartner') or node.get('accountingDocument')
                            if node_id_val and node_id_val not in nodes:
                                nodes[node_id_val] = {
                                    "id": node_id_val,
                                    "label": list(node.labels)[0] if node.labels else "Unknown",
                                    "properties": {k: str(v)[:100] for k, v in dict(node).items() if v is not None}
                                }
                        
                        for rel in path.relationships:
                            start_id = rel.start_node.get('salesOrder') or rel.start_node.get('billingDocument') or \
                                    rel.start_node.get('deliveryDocument') or rel.start_node.get('product') or \
                                    rel.start_node.get('businessPartner') or rel.start_node.get('accountingDocument')
                            end_id = rel.end_node.get('salesOrder') or rel.end_node.get('billingDocument') or \
                                    rel.end_node.get('deliveryDocument') or rel.end_node.get('product') or \
                                    rel.end_node.get('businessPartner') or rel.end_node.get('accountingDocument')
                            
                            edges.append({
                                "source": start_id,
                                "target": end_id,
                                "type": rel.type,
                                "properties": dict(rel)
                            })
                
                result_data = {"nodes": list(nodes.values()), "edges": edges}
                
                # Cache the result
                await cache_service.cache_graph(node_id, depth, result_data)
                
                return result_data
        except Exception as e:
            print(f"Error getting neighbors: {e}")
            return {"nodes": [], "edges": []}
    
    async def get_schema(self) -> Dict:
        """Get graph schema"""
        try:
            driver = await self._get_driver()
            
            # Get all node labels
            query_labels = "CALL db.labels() YIELD label RETURN label"
            async with driver.session() as session:
                result = await session.run(query_labels)
                labels = [record['label'] for record in await result.data()]
            
            # Get all relationship types
            query_rels = "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType"
            async with driver.session() as session:
                result = await session.run(query_rels)
                rel_types = [record['relationshipType'] for record in await result.data()]
            
            # Get node counts
            node_counts = await self.get_node_counts()
            
            return {
                "node_types": labels,
                "relationship_types": rel_types,
                "node_counts": node_counts
            }
        except Exception as e:
            print(f"Error getting schema: {e}")
            return {
                "node_types": [],
                "relationship_types": [],
                "node_counts": {}
            }
    
    async def search_nodes(self, search_query: str, node_type: Optional[str], limit: int) -> List[Dict]:
        """Search nodes by text in properties"""
        try:
            driver = await self._get_driver()
            
            # Make search case-insensitive and more flexible
            search_pattern = f"(?i).*{re.escape(search_query)}.*"
            
            # Build search query with better matching
            if node_type:
                cypher = f"""
                MATCH (n:{node_type})
                WHERE ANY(prop IN keys(n) 
                        WHERE n[prop] IS NOT NULL 
                        AND toString(n[prop]) =~ $pattern)
                RETURN n
                LIMIT $limit
                """
            else:
                cypher = """
                MATCH (n)
                WHERE ANY(prop IN keys(n) 
                        WHERE n[prop] IS NOT NULL 
                        AND toString(n[prop]) =~ $pattern)
                RETURN n
                LIMIT $limit
                """
            
            async with driver.session() as session:
                result = await session.run(cypher, pattern=search_pattern, limit=limit)
                records = await result.data()
                
                nodes = []
                for record in records:
                    node = record['n']
                    # Extract node ID
                    node_id = None
                    for id_field in ['salesOrder', 'billingDocument', 'deliveryDocument', 
                                    'product', 'businessPartner', 'accountingDocument']:
                        if id_field in node:
                            node_id = node[id_field]
                            break
                    
                    if not node_id:
                        node_id = str(node.element_id)
                    
                    nodes.append({
                        "id": node_id,
                        "type": list(node.labels)[0] if node.labels else "Unknown",
                        "properties": {k: str(v)[:100] for k, v in dict(node).items() if v}
                    })
                
                print(f"Search for '{search_query}' found {len(nodes)} nodes")
                return nodes
        except Exception as e:
            print(f"Error searching nodes: {e}")
            return []
    
    async def execute_cypher(self, query: str) -> List[Dict]:
        """Execute a Cypher query and return results"""
        try:
            driver = await self._get_driver()
            
            async with driver.session() as session:
                result = await session.run(query)
                records = await result.data()
                return records
        except Exception as e:
            raise Exception(f"Cypher execution failed: {str(e)}")
    
    async def get_graph_overview(self) -> Dict:
        """
        Get aggregated overview of the graph - returns summary nodes with counts
        instead of all individual nodes for initial visualization.
        """
        try:
            driver = await self._get_driver()
            
            # Get counts by node type with aggregated information
            query = """
            MATCH (n)
            WITH labels(n)[0] as type, count(*) as count
            RETURN type, count
            ORDER BY count DESC
            """
            
            async with driver.session() as session:
                result = await session.run(query)
                type_counts = await result.data()
                
                # Create aggregated nodes
                nodes = []
                for tc in type_counts:
                    node_type = tc['type']
                    count = tc['count']
                    
                    # Get sample properties for this type to show in tooltip
                    sample_query = f"""
                    MATCH (n:{node_type})
                    RETURN n
                    LIMIT 1
                    """
                    sample_result = await session.run(sample_query)
                    sample_record = await sample_result.single()
                    
                    sample_props = {}
                    if sample_record:
                        sample_node = sample_record['n']
                        # Get only important properties for display
                        important_keys = ['productGroup', 'businessPartnerName', 'totalNetAmount', 
                                        'transactionCurrency', 'shippingPoint']
                        for key in important_keys:
                            if key in sample_node:
                                sample_props[key] = str(sample_node[key])[:50]
                    
                    # Create a summary node for this type
                    nodes.append({
                        "id": f"summary_{node_type}",
                        "name": f"{node_type} ({count})",
                        "type": "Summary",
                        "original_type": node_type,
                        "count": count,
                        "sample_properties": sample_props,
                        "properties": {
                            "type": node_type,
                            "total_count": count,
                            "sample": sample_props
                        }
                    })
                
                # Create summary relationships between types based on actual connections
                rel_query = """
                MATCH (a)-[r]->(b)
                WITH labels(a)[0] as source_type, labels(b)[0] as target_type, 
                    type(r) as rel_type, count(*) as count
                WHERE source_type IS NOT NULL AND target_type IS NOT NULL
                RETURN source_type, target_type, rel_type, count
                ORDER BY count DESC
                LIMIT 30
                """
                
                rel_result = await session.run(rel_query)
                rel_data = await rel_result.data()
                
                links = []
                for rel in rel_data:
                    links.append({
                        "source": f"summary_{rel['source_type']}",
                        "target": f"summary_{rel['target_type']}",
                        "type": rel['rel_type'],
                        "count": rel['count'],
                        "label": f"{rel['rel_type']} ({rel['count']})"
                    })
                
                return {
                    "nodes": nodes,
                    "links": links,
                    "total_nodes": sum(tc['count'] for tc in type_counts),
                    "type_counts": {tc['type']: tc['count'] for tc in type_counts}
                }
                
        except Exception as e:
            print(f"Error getting graph overview: {e}")
            return {"nodes": [], "links": [], "total_nodes": 0, "type_counts": {}}
    def extract_node_ids(self, results: List[Dict]) -> List[str]:
        """Extract node IDs from query results for highlighting"""
        node_ids = []
        for record in results:
            for key, value in record.items():
                # Look for common ID fields
                if isinstance(value, str) and any(
                    id_field in key.lower() 
                    for id_field in ['id', 'document', 'order', 'product', 'customer', 'partner']
                ):
                    if value and value not in node_ids:
                        node_ids.append(value)
        return node_ids[:20]  # Limit to 20 nodes for highlighting
    
    async def get_subgraph(self, node_ids: List[str]) -> Dict:
        """Get subgraph containing specified nodes"""
        try:
            driver = await self._get_driver()
            
            # Create a query to get all paths between the specified nodes
            query = """
            MATCH path = (n)-[*1..2]-(m)
            WHERE n.salesOrder IN $node_ids OR n.billingDocument IN $node_ids 
               OR n.deliveryDocument IN $node_ids OR n.product IN $node_ids
               OR n.businessPartner IN $node_ids OR n.accountingDocument IN $node_ids
            AND m.salesOrder IN $node_ids OR m.billingDocument IN $node_ids 
               OR m.deliveryDocument IN $node_ids OR m.product IN $node_ids
               OR m.businessPartner IN $node_ids OR m.accountingDocument IN $node_ids
            RETURN path
            """
            
            async with driver.session() as session:
                result = await session.run(query, node_ids=node_ids)
                records = await result.data()
                
                nodes = {}
                edges = []
                
                for record in records:
                    if 'path' in record:
                        path = record['path']
                        for node in path.nodes:
                            node_id_val = node.get('salesOrder') or node.get('billingDocument') or \
                                          node.get('deliveryDocument') or node.get('product') or \
                                          node.get('businessPartner') or node.get('accountingDocument')
                            if node_id_val and node_id_val not in nodes:
                                nodes[node_id_val] = {
                                    "id": node_id_val,
                                    "label": list(node.labels)[0] if node.labels else "Unknown",
                                    "properties": {k: str(v)[:100] for k, v in dict(node).items() if v}
                                }
                        
                        for rel in path.relationships:
                            start_id = rel.start_node.get('salesOrder') or rel.start_node.get('billingDocument') or \
                                       rel.start_node.get('deliveryDocument') or rel.start_node.get('product') or \
                                       rel.start_node.get('businessPartner') or rel.start_node.get('accountingDocument')
                            end_id = rel.end_node.get('salesOrder') or rel.end_node.get('billingDocument') or \
                                     rel.end_node.get('deliveryDocument') or rel.end_node.get('product') or \
                                     rel.end_node.get('businessPartner') or rel.end_node.get('accountingDocument')
                            
                            edges.append({
                                "source": start_id,
                                "target": end_id,
                                "type": rel.type,
                                "properties": dict(rel)
                            })
                
                return {
                    "nodes": list(nodes.values()),
                    "edges": edges
                }
        except Exception as e:
            print(f"Error getting subgraph: {e}")
            return {"nodes": [], "edges": []}