#!/usr/bin/env python
"""
Comprehensive Test Suite for Graph Business System
Run with: python test_runner.py
"""

import asyncio
import json
import sys
from datetime import datetime
from typing import Dict, List, Any
import httpx
import time

class TestRunner:
    def __init__(self):
        self.base_url = "http://localhost:8000"
        self.client = httpx.AsyncClient(timeout=30.0)
        self.test_results = []
        self.test_count = 0
        self.passed = 0
        self.failed = 0
        
    def log_test(self, name: str, passed: bool, message: str = ""):
        """Log test result"""
        status = "✅ PASS" if passed else "❌ FAIL"
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] {status} - {name}")
        if message:
            print(f"  └─ {message}")
        self.test_count += 1
        if passed:
            self.passed += 1
        else:
            self.failed += 1
            
        self.test_results.append({
            "name": name,
            "passed": passed,
            "message": message,
            "timestamp": timestamp
        })
    
    async def test_health(self):
        """Test 1: Health Check"""
        try:
            response = await self.client.get(f"{self.base_url}/health")
            data = response.json()
            
            # Check all critical services
            neo4j_ok = data["services"].get("neo4j") == "connected"
            graph_ok = data["services"].get("graph") == "available"
            
            passed = response.status_code == 200 and neo4j_ok and graph_ok
            self.log_test(
                "Health Check",
                passed,
                f"Neo4j: {data['services'].get('neo4j')}, Graph: {data['services'].get('graph')}"
            )
        except Exception as e:
            self.log_test("Health Check", False, str(e))
    
    async def test_schema(self):
        """Test 2: Schema Endpoint"""
        try:
            response = await self.client.get(f"{self.base_url}/api/graph/schema")
            data = response.json()
            
            passed = (
                response.status_code == 200 and
                len(data.get("node_types", [])) > 0 and
                len(data.get("relationship_types", [])) > 0
            )
            
            self.log_test(
                "Schema Endpoint",
                passed,
                f"Node types: {len(data.get('node_types', []))}, "
                f"Relationship types: {len(data.get('relationship_types', []))}"
            )
        except Exception as e:
            self.log_test("Schema Endpoint", False, str(e))
    
    async def test_statistics(self):
        """Test 3: Statistics Endpoint"""
        try:
            response = await self.client.get(f"{self.base_url}/api/graph/statistics")
            data = response.json()
            
            passed = (
                response.status_code == 200 and
                data.get("total_nodes", 0) > 0 and
                data.get("total_relationships", 0) > 0
            )
            
            self.log_test(
                "Statistics Endpoint",
                passed,
                f"Total nodes: {data.get('total_nodes', 0)}, "
                f"Total relationships: {data.get('total_relationships', 0)}"
            )
        except Exception as e:
            self.log_test("Statistics Endpoint", False, str(e))
    
    async def test_nodes_pagination(self):
        """Test 4: Nodes Pagination"""
        try:
            # Test first page
            response = await self.client.get(
                f"{self.base_url}/api/graph/nodes",
                params={"limit": 10, "skip": 0}
            )
            data_page1 = response.json()
            
            # Test second page
            response = await self.client.get(
                f"{self.base_url}/api/graph/nodes",
                params={"limit": 10, "skip": 10}
            )
            data_page2 = response.json()
            
            passed = (
                response.status_code == 200 and
                len(data_page1) > 0 and
                len(data_page2) > 0 and
                data_page1[0]["id"] != data_page2[0]["id"]
            )
            
            self.log_test(
                "Nodes Pagination",
                passed,
                f"Page1: {len(data_page1)} nodes, Page2: {len(data_page2)} nodes"
            )
        except Exception as e:
            self.log_test("Nodes Pagination", False, str(e))
    
    async def test_node_metadata(self):
        """Test 5: Node Metadata"""
        try:
            # Get a sample node first
            response = await self.client.get(
                f"{self.base_url}/api/graph/nodes",
                params={"limit": 1}
            )
            nodes = response.json()
            
            if nodes and len(nodes) > 0:
                node_id = nodes[0]["id"]
                response = await self.client.get(
                    f"{self.base_url}/api/graph/node/{node_id}"
                )
                data = response.json()
                
                passed = (
                    response.status_code == 200 and
                    data.get("id") == node_id and
                    "properties" in data
                )
                
                self.log_test(
                    "Node Metadata",
                    passed,
                    f"Node {node_id} - Type: {data.get('type', 'Unknown')}"
                )
            else:
                self.log_test("Node Metadata", False, "No nodes found")
        except Exception as e:
            self.log_test("Node Metadata", False, str(e))
    
    async def test_graph_overview(self):
        """Test 6: Graph Overview"""
        try:
            response = await self.client.get(f"{self.base_url}/api/graph/overview")
            data = response.json()
            
            passed = (
                response.status_code == 200 and
                len(data.get("nodes", [])) > 0 and
                data.get("total_nodes", 0) > 0
            )
            
            self.log_test(
                "Graph Overview",
                passed,
                f"Summary nodes: {len(data.get('nodes', []))}, "
                f"Total: {data.get('total_nodes', 0)}"
            )
        except Exception as e:
            self.log_test("Graph Overview", False, str(e))
    
    async def test_search_nodes(self):
        """Test 7: Search Functionality"""
        try:
            # Search for a common term
            response = await self.client.get(
                f"{self.base_url}/api/graph/search",
                params={"query": "order", "limit": 5}
            )
            data = response.json()
            
            passed = response.status_code == 200 and len(data) > 0
            
            self.log_test(
                "Search Functionality",
                passed,
                f"Found {len(data)} results for 'order'"
            )
        except Exception as e:
            self.log_test("Search Functionality", False, str(e))
    
    async def test_neighbors_expansion(self):
        """Test 8: Node Neighbors Expansion"""
        try:
            # Get a sales order node first
            response = await self.client.get(
                f"{self.base_url}/api/graph/nodes",
                params={"node_type": "SalesOrder", "limit": 1}
            )
            nodes = response.json()
            
            if nodes and len(nodes) > 0:
                node_id = nodes[0]["id"]
                response = await self.client.get(
                    f"{self.base_url}/api/graph/neighbors/{node_id}",
                    params={"depth": 1, "limit": 10}
                )
                data = response.json()
                
                passed = (
                    response.status_code == 200 and
                    len(data.get("nodes", [])) >= 0  # Could be 0 if no neighbors
                )
                
                self.log_test(
                    "Node Neighbors Expansion",
                    passed,
                    f"Node {node_id} has {len(data.get('nodes', []))} neighbors"
                )
            else:
                self.log_test("Node Neighbors Expansion", False, "No SalesOrder found")
        except Exception as e:
            self.log_test("Node Neighbors Expansion", False, str(e))
    
    async def test_guardrails_offtopic(self):
        """Test 9: Guardrails - Off-topic Query"""
        try:
            response = await self.client.post(
                f"{self.base_url}/api/chat/query",
                json={"question": "What's the weather today?"}
            )
            data = response.json()
            
            passed = (
                response.status_code == 200 and
                "only answer questions about business" in data.get("answer", "").lower()
            )
            
            self.log_test(
                "Guardrails - Off-topic",
                passed,
                f"Response: {data.get('answer', '')[:50]}..."
            )
        except Exception as e:
            self.log_test("Guardrails - Off-topic", False, str(e))
    
    async def test_guardrails_creative(self):
        """Test 10: Guardrails - Creative Writing"""
        try:
            response = await self.client.post(
                f"{self.base_url}/api/chat/query",
                json={"question": "Write a poem about orders"}
            )
            data = response.json()
            
            passed = (
                response.status_code == 200 and
                "only answer questions about business" in data.get("answer", "").lower()
            )
            
            self.log_test(
                "Guardrails - Creative Writing",
                passed,
                "Successfully rejected creative request"
            )
        except Exception as e:
            self.log_test("Guardrails - Creative Writing", False, str(e))
    
    async def test_product_billing_query(self):
        """Test 11: Example Query - Products with highest billing"""
        try:
            response = await self.client.post(
                f"{self.base_url}/api/chat/query",
                json={"question": "Which products have the highest billing volume?"}
            )
            data = response.json()
            
            passed = (
                response.status_code == 200 and
                data.get("success", False) and
                "product" in data.get("answer", "").lower()
            )
            
            self.log_test(
                "Example Query - Product Billing",
                passed,
                f"Found results: {len(data.get('data', []))} records"
            )
        except Exception as e:
            self.log_test("Example Query - Product Billing", False, str(e))
    
    async def test_broken_flows_query(self):
        """Test 12: Example Query - Broken flows"""
        try:
            response = await self.client.post(
                f"{self.base_url}/api/chat/query",
                json={"question": "Find sales orders that have been delivered but not billed"}
            )
            data = response.json()
            
            passed = (
                response.status_code == 200 and
                data.get("success", False)
            )
            
            self.log_test(
                "Example Query - Broken Flows",
                passed,
                f"Found {len(data.get('data', []))} incomplete orders"
            )
        except Exception as e:
            self.log_test("Example Query - Broken Flows", False, str(e))
    
    async def test_trace_flow_query(self):
        """Test 13: Example Query - Trace flow"""
        try:
            # Use a sample billing document ID
            response = await self.client.post(
                f"{self.base_url}/api/chat/query",
                json={"question": "Trace the full flow for billing document 90504248"}
            )
            data = response.json()
            
            passed = (
                response.status_code == 200 and
                data.get("success", False)
            )
            
            self.log_test(
                "Example Query - Trace Flow",
                passed,
                "Successfully traced document flow"
            )
        except Exception as e:
            self.log_test("Example Query - Trace Flow", False, str(e))
    
    async def test_customer_analytics_query(self):
        """Test 14: Additional Query - Customer Analytics"""
        try:
            response = await self.client.post(
                f"{self.base_url}/api/chat/query",
                json={"question": "Which customers have the highest total order value?"}
            )
            data = response.json()
            
            passed = (
                response.status_code == 200 and
                data.get("success", False)
            )
            
            self.log_test(
                "Additional Query - Customer Analytics",
                passed,
                f"Found {len(data.get('data', []))} customers"
            )
        except Exception as e:
            self.log_test("Additional Query - Customer Analytics", False, str(e))
    
    async def test_revenue_query(self):
        """Test 15: Additional Query - Revenue Summary"""
        try:
            response = await self.client.post(
                f"{self.base_url}/api/chat/query",
                json={"question": "What is the total revenue from billing documents?"}
            )
            data = response.json()
            
            passed = (
                response.status_code == 200 and
                data.get("success", False)
            )
            
            self.log_test(
                "Additional Query - Revenue Summary",
                passed,
                "Successfully calculated revenue"
            )
        except Exception as e:
            self.log_test("Additional Query - Revenue Summary", False, str(e))
    
    async def test_rate_limiting(self):
        """Test 16: Rate Limiting"""
        try:
            # Make 35 rapid requests to a rate-limited endpoint (not health)
            results = []
            for i in range(35):
                response = await self.client.get(
                    f"{self.base_url}/api/graph/nodes",
                    params={"limit": 1}
                )
                results.append(response.status_code)
                await asyncio.sleep(0.05)  # Small delay to not overwhelm
            
            # Check if any request got rate limited (429)
            rate_limited = 429 in results
            
            passed = rate_limited
            
            limited_at = results.index(429) if 429 in results else "never"
            self.log_test(
                "Rate Limiting",
                passed,
                f"Rate limited after {limited_at} requests"
            )
        except Exception as e:
            self.log_test("Rate Limiting", False, str(e))
    
    async def test_caching(self):
        """Test 17: Cache Functionality"""
        try:
            # First request - should be slow
            start = time.time()
            response1 = await self.client.get(
                f"{self.base_url}/api/graph/overview"
            )
            time1 = time.time() - start
            
            # Second request - should be faster (cached)
            start = time.time()
            response2 = await self.client.get(
                f"{self.base_url}/api/graph/overview"
            )
            time2 = time.time() - start
            
            passed = time2 < time1 * 0.8  # Second request should be faster
            
            self.log_test(
                "Cache Functionality",
                passed,
                f"First: {time1:.2f}s, Second: {time2:.2f}s"
            )
        except Exception as e:
            self.log_test("Cache Functionality", False, str(e))
    
    async def test_data_model_validation(self):
        """Test 18: Data Model Validation"""
        try:
            # Check relationship types existence
            response = await self.client.get(f"{self.base_url}/api/graph/schema")
            schema = response.json()
            
            required_relationships = [
                "HAS_ORDER_ITEM", "HAS_DELIVERY_ITEM", "HAS_BILLING_ITEM",
                "REFERENCES_PRODUCT", "PLACED_ORDER", "DELIVERS_ORDER",
                "BILLS_PRODUCT", "GENERATES_JOURNAL_ENTRY", "RECEIVES_PAYMENT"
            ]
            
            existing_rels = schema.get("relationship_types", [])
            missing = [r for r in required_relationships if r not in existing_rels]
            
            passed = len(missing) == 0
            
            self.log_test(
                "Data Model Validation",
                passed,
                f"Present: {len(existing_rels)} relationships, Missing: {len(missing)}"
            )
            if missing:
                print(f"  ⚠️ Missing: {missing}")
                
        except Exception as e:
            self.log_test("Data Model Validation", False, str(e))
    
    async def run_all_tests(self):
        """Run all tests"""
        print("\n" + "="*60)
        print("🧪 GRAPH BUSINESS SYSTEM - COMPREHENSIVE TEST SUITE")
        print("="*60 + "\n")
        
        start_time = time.time()
        
        # Run all tests
        await self.test_health()
        await self.test_schema()
        await self.test_statistics()
        await self.test_nodes_pagination()
        await self.test_node_metadata()
        await self.test_graph_overview()
        await self.test_search_nodes()
        await self.test_neighbors_expansion()
        await self.test_guardrails_offtopic()
        await self.test_guardrails_creative()
        await self.test_product_billing_query()
        await self.test_broken_flows_query()
        await self.test_trace_flow_query()
        await self.test_customer_analytics_query()
        await self.test_revenue_query()
        await self.test_rate_limiting()
        await self.test_caching()
        await self.test_data_model_validation()
        
        elapsed = time.time() - start_time
        
        # Print summary
        print("\n" + "="*60)
        print("📊 TEST SUMMARY")
        print("="*60)
        print(f"Total Tests: {self.test_count}")
        print(f"✅ Passed: {self.passed}")
        print(f"❌ Failed: {self.failed}")
        print(f"📈 Success Rate: {(self.passed/self.test_count)*100:.1f}%")
        print(f"⏱️  Time: {elapsed:.2f}s")
        
        # Save results to file
        with open("test_results.json", "w") as f:
            json.dump({
                "timestamp": datetime.now().isoformat(),
                "total": self.test_count,
                "passed": self.passed,
                "failed": self.failed,
                "success_rate": (self.passed/self.test_count)*100,
                "results": self.test_results
            }, f, indent=2)
        
        print("\n📁 Results saved to test_results.json")
        
        # Close client
        await self.client.aclose()
        
        return self.failed == 0

async def main():
    runner = TestRunner()
    success = await runner.run_all_tests()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    asyncio.run(main())