import re
import os
from typing import Tuple, Optional, List, Dict
import json
from groq import Groq
from functools import lru_cache
import hashlib
import asyncio
from datetime import datetime

class LLMService:
    def __init__(self):
        self.api_key = os.getenv("GROQ_API_KEY")
        self.client = None
        self.use_mock = not self.api_key
        self.cache = {}
        
        if not self.use_mock:
            try:
                self.client = Groq(api_key=self.api_key)
                print("✅ Groq client initialized successfully")
            except Exception as e:
                print(f"⚠️ Failed to initialize Groq client: {e}")
                self.use_mock = True
        else:
            print("⚠️ No GROQ_API_KEY found, using mock responses")
        
        # Build schema with ACTUAL relationship types from your database
        self.schema = self._build_schema()
        self.example_queries = self._build_examples()
    
    def _build_schema(self):
        """Build comprehensive graph schema with ACTUAL relationship types"""
        return """
        # Neo4j Graph Schema for Business System
        
        ## Node Types with Properties:
        
        **SalesOrder**
        - salesOrder (string, primary key)
        - totalNetAmount (float)
        - creationDate (string)
        - soldToParty (string, references Customer)
        - overallDeliveryStatus (string)
        
        **SalesOrderItem**
        - salesOrder (string, references SalesOrder)
        - salesOrderItem (string)
        - material (string, references Product)
        - requestedQuantity (float)
        - netAmount (float)
        
        **BillingDocument**
        - billingDocument (string, primary key)
        - totalNetAmount (float)
        - accountingDocument (string)
        - isCancelled (boolean)
        - soldToParty (string)
        
        **BillingDocumentItem**
        - billingDocument (string, references BillingDocument)
        - billingDocumentItem (string)
        - material (string, references Product)
        - billingQuantity (float)
        - netAmount (float)
        - referenceSdDocument (string, references SalesOrder)
        
        **Delivery**
        - deliveryDocument (string, primary key)
        - shippingPoint (string)
        - overallGoodsMovementStatus (string)
        - creationDate (string)
        
        **DeliveryItem**
        - deliveryDocument (string, references Delivery)
        - deliveryDocumentItem (string)
        - actualDeliveryQuantity (float)
        - referenceSdDocument (string, references SalesOrder)
        - referenceSdDocumentItem (string)
        
        **Product**
        - product (string, primary key)
        - productType (string)
        - productGroup (string)
        
        **Customer**
        - businessPartner (string, primary key)
        - businessPartnerName (string)
        
        **JournalEntry**
        - accountingDocument (string, primary key)
        - amountInTransactionCurrency (float)
        
        **Payment**
        - accountingDocument (string, primary key)
        - clearingDate (string)
        - amountInTransactionCurrency (float)
        
        ## ACTUAL Relationship Types in Database:
        
        - (SalesOrder)-[:HAS_ORDER_ITEM]->(SalesOrderItem)
        - (SalesOrderItem)-[:REFERENCES_PRODUCT]->(Product)
        - (Delivery)-[:HAS_DELIVERY_ITEM]->(DeliveryItem)
        - (DeliveryItem)-[:DELIVERS_ORDER]->(SalesOrder)  # NOT FULFILLS_ORDER_ITEM
        - (BillingDocument)-[:HAS_BILLING_ITEM]->(BillingDocumentItem)
        - (BillingDocumentItem)-[:BILLS_PRODUCT]->(Product)  # NOT BILLS_ORDER_ITEM
        - (Customer)-[:PLACED_ORDER]->(SalesOrder)
        - (Customer)-[:HAS_BILLING_DOCUMENT]->(BillingDocument)
        - (BillingDocument)-[:GENERATES_JOURNAL_ENTRY]->(JournalEntry)
        - (BillingDocument)-[:RECEIVES_PAYMENT]->(Payment)
        - (BillingDocument)-[:CANCELLED_BY]->(BillingDocument)
        - (Delivery)-[:SHIPPED_VIA]->(SalesOrder)
        
        ## Important Notes:
        - Delivery connects to SalesOrder through DELIVERS_ORDER relationship
        - BillingDocumentItem connects to Product through BILLS_PRODUCT relationship
        - Use toFloat() for numeric properties
        - Always add LIMIT to prevent large result sets (default 20)
        """
    
    def _build_examples(self):
        """Build example queries with ACTUAL relationship types"""
        return [
            {
                "question": "Which products have the highest billing volume?",
                "query": """
                MATCH (bi:BillingDocumentItem)-[:BILLS_PRODUCT]->(p:Product)
                WITH p, COUNT(bi) as billing_count, SUM(toFloat(bi.netAmount)) as total_value
                RETURN p.product as product_id, 
                       p.productGroup as product_group,
                       billing_count as number_of_billing_documents,
                       total_value as total_billed_amount
                ORDER BY billing_count DESC
                LIMIT 10
                """
            },
            {
                "question": "Show me all sales orders that haven't been delivered",
                "query": """
                MATCH (s:SalesOrder)
                WHERE NOT EXISTS {
                    MATCH (s)<-[:DELIVERS_ORDER]-(:Delivery)
                }
                RETURN s.salesOrder as sales_order, 
                       toFloat(s.totalNetAmount) as amount,
                       s.creationDate as order_date
                ORDER BY s.creationDate DESC
                LIMIT 20
                """
            },
            {
                "question": "Find orders that were delivered but not billed",
                "query": """
                MATCH (d:Delivery)-[:DELIVERS_ORDER]->(s:SalesOrder)
                WHERE NOT EXISTS {
                    MATCH (s)<-[:BILLS_PRODUCT]-(:BillingDocumentItem)
                }
                RETURN s.salesOrder as sales_order,
                       d.deliveryDocument as delivery_document
                LIMIT 20
                """
            },
            {
                "question": "Which customers have the highest total order value?",
                "query": """
                MATCH (c:Customer)-[:PLACED_ORDER]->(s:SalesOrder)
                WITH c, COUNT(s) as order_count, SUM(toFloat(s.totalNetAmount)) as total_value
                RETURN c.businessPartner as customer_id,
                       c.businessPartnerName as customer_name,
                       order_count,
                       total_value
                ORDER BY total_value DESC
                LIMIT 10
                """
            },
            {
                "question": "What is the total revenue from billing documents?",
                "query": """
                MATCH (b:BillingDocument)
                WHERE b.isCancelled = false
                RETURN SUM(toFloat(b.totalNetAmount)) as total_revenue,
                       COUNT(b) as total_invoices,
                       AVG(toFloat(b.totalNetAmount)) as average_invoice_value
                """
            },
            {
                "question": "Trace the full flow for billing document {id}",
                "query": """
                MATCH (b:BillingDocument {billingDocument: $doc_id})
                OPTIONAL MATCH (b)-[:HAS_BILLING_ITEM]->(bi:BillingDocumentItem)
                OPTIONAL MATCH (bi)-[:BILLS_PRODUCT]->(p:Product)
                OPTIONAL MATCH (b)-[:GENERATES_JOURNAL_ENTRY]->(j:JournalEntry)
                OPTIONAL MATCH (b)-[:RECEIVES_PAYMENT]->(pay:Payment)
                OPTIONAL MATCH (b)-[:CANCELLED_BY]->(cancelled:BillingDocument)
                OPTIONAL MATCH (c:Customer)-[:HAS_BILLING_DOCUMENT]->(b)
                RETURN b.billingDocument as billing_document,
                       bi.billingDocumentItem as billing_item,
                       p.product as product,
                       j.accountingDocument as journal_entry,
                       pay.accountingDocument as payment,
                       pay.clearingDate as payment_date,
                       c.businessPartner as customer,
                       cancelled.billingDocument as cancelled_by
                """
            }
        ]
    
    def validate_query(self, question: str) -> Tuple[bool, str]:
        """Validate that the query is relevant to the business domain"""
        question_lower = question.lower()
        
        # Business keywords
        business_keywords = [
            "order", "delivery", "bill", "invoice", "payment", "product", 
            "customer", "shipment", "journal", "account", "sales", "revenue",
            "transaction", "document", "material", "plant", "supplier",
            "show", "find", "list", "get", "what", "which", "how many",
            "total", "average", "sum", "count", "top", "highest", "lowest"
        ]
        
        # Off-topic patterns
        off_topic_patterns = [
            r"(weather|stock market|news|sports|politics|movie|recipe|poem)",
            r"(who is|what is the capital|history of|president of)",
            r"(write a|create a story|tell me about yourself)",
            r"(covid|pandemic|election|war)"
        ]
        
        for pattern in off_topic_patterns:
            if re.search(pattern, question_lower):
                return False, "🔒 I can only answer questions about business operations (orders, deliveries, billing, payments, products, customers)."
        
        if len(question_lower.split()) > 2:
            has_business_keyword = any(keyword in question_lower for keyword in business_keywords)
            if not has_business_keyword:
                return False, "📊 I specialize in business data queries. Please ask about orders, deliveries, billing, payments, products, or customers."
        
        return True, ""
    
    async def generate_cypher(self, question: str) -> str:
        """Generate Cypher query from natural language"""
        cache_key = hashlib.md5(question.encode()).hexdigest()
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        if self.use_mock:
            cypher = self._mock_cypher_generation(question)
            self.cache[cache_key] = cypher
            return cypher
        
        try:
            relevant_examples = self._find_relevant_examples(question)
            examples_text = self._format_examples(relevant_examples)
            
            prompt = f"""You are a Neo4j Cypher query expert. Convert the following question to a Cypher query.

{self.schema}

{examples_text}

Rules:
1. Return ONLY the Cypher query, no explanation
2. Use ACTUAL relationship types: DELIVERS_ORDER, BILLS_PRODUCT, etc.
3. Use toFloat() for numeric properties
4. Add LIMIT 20 if not aggregation

Question: {question}

Cypher Query:"""
            
            response = self.client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "system", "content": "You are a Neo4j Cypher query generator. Use the exact relationship types provided."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=500
            )
            
            cypher_query = response.choices[0].message.content.strip()
            cypher_query = cypher_query.replace('```cypher', '').replace('```', '').strip()
            
            if 'LIMIT' not in cypher_query.upper() and 'COUNT' not in cypher_query.upper():
                cypher_query += " LIMIT 20"
            
            self.cache[cache_key] = cypher_query
            return cypher_query
            
        except Exception as e:
            print(f"Error generating Cypher: {e}")
            return self._mock_cypher_generation(question)
    # Add this method to LLMService class

    async def generate_cypher_stream(self, question: str):
        """Generate Cypher query with streaming"""
        if self.use_mock:
            yield self._mock_cypher_generation(question)
            return
        
        try:
            relevant_examples = self._find_relevant_examples(question)
            examples_text = self._format_examples(relevant_examples)
            
            prompt = f"""You are a Neo4j Cypher query expert. Convert the following question to a Cypher query.

    {self.schema}

    {examples_text}

    Rules:
    1. Return ONLY the Cypher query, no explanation or markdown formatting
    2. Use proper MATCH patterns with node labels
    3. Add LIMIT 20 if not an aggregation query
    4. Use toFloat() for numeric properties stored as strings

    Question: {question}

    Cypher Query:"""
            
            response = self.client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "system", "content": "You are a Neo4j Cypher query generator."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=500,
                stream=True
            )
            
            full_response = ""
            for chunk in response:
                if chunk.choices[0].delta.content:
                    content = chunk.choices[0].delta.content
                    full_response += content
                    yield content
            
            # Cache the result
            cache_key = hashlib.md5(question.encode()).hexdigest()
            self.cache[cache_key] = full_response.strip()
            
        except Exception as e:
            print(f"Error in streaming: {e}")
            yield self._mock_cypher_generation(question)

    async def generate_response_stream(self, question: str, data: List[Dict]):
        """Generate natural language response with streaming"""
        if not data:
            yield "📭 No results found for your query. Try rephrasing or checking the data."
            return
        
        if self.use_mock:
            yield self._mock_response_generation(question, data)
            return
        
        try:
            result_count = len(data)
            data_sample = data[:10]
            data_text = json.dumps(data_sample, indent=2, default=str)
            
            prompt = f"""You are a business analyst. Generate a concise response based on the query results.

    Question: {question}
    Number of results: {result_count}
    Sample data:
    {data_text}

    Generate a clear, professional response with key findings. Use markdown for formatting."""
            
            response = self.client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "system", "content": "You are a business analyst providing data-driven answers."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=400,
                stream=True
            )
            
            for chunk in response:
                if chunk.choices[0].delta.content:
                    yield chunk.choices[0].delta.content
                    
        except Exception as e:
            print(f"Error in streaming response: {e}")
            yield self._mock_response_generation(question, data)

    def _find_relevant_examples(self, question: str) -> List[Dict]:
        """Find relevant examples based on keywords"""
        question_lower = question.lower()
        relevant = []
        
        keywords_map = {
            "product": [0],
            "billing": [0, 5],
            "order": [1, 2, 3],
            "delivery": [2],
            "customer": [3],
            "revenue": [4],
            "flow": [5],
            "trace": [5]
        }
        
        matched_indices = set()
        for keyword, indices in keywords_map.items():
            if keyword in question_lower:
                matched_indices.update(indices)
        
        for idx in list(matched_indices)[:2]:
            if idx < len(self.example_queries):
                relevant.append(self.example_queries[idx])
        
        if not relevant and len(self.example_queries) >= 2:
            relevant = self.example_queries[:2]
        
        return relevant
    
    def _format_examples(self, examples: List[Dict]) -> str:
        if not examples:
            return ""
        text = "Examples:\n"
        for ex in examples:
            text += f"Q: {ex['question']}\n"
            text += f"A: {ex['query']}\n\n"
        return text
    
    def _mock_cypher_generation(self, question: str) -> str:
        """Fallback mock Cypher generation with CORRECT relationship types"""
        question_lower = question.lower()
        
        # Products with highest billing - using BILLS_PRODUCT
        if "product" in question_lower and ("highest" in question_lower or "billing" in question_lower):
            return """
            MATCH (bi:BillingDocumentItem)-[:BILLS_PRODUCT]->(p:Product)
            WITH p, COUNT(bi) as billing_count, SUM(toFloat(bi.netAmount)) as total_value
            RETURN p.product as product_id, 
                   p.productGroup as product_group,
                   billing_count as number_of_billing_documents, 
                   total_value as total_billed_amount
            ORDER BY billing_count DESC
            LIMIT 10
            """
        
        # Broken flows - using DELIVERS_ORDER
        if "broken" in question_lower or "incomplete" in question_lower or ("delivered" in question_lower and "billed" in question_lower):
            return """
            MATCH (d:Delivery)-[:DELIVERS_ORDER]->(s:SalesOrder)
            WHERE NOT EXISTS {
                MATCH (s)<-[:BILLS_PRODUCT]-(:BillingDocumentItem)
            }
            RETURN s.salesOrder as sales_order,
                   d.deliveryDocument as delivery_document
            LIMIT 20
            """
        
        # Customer orders
        if "customer" in question_lower and ("order" in question_lower or "value" in question_lower):
            return """
            MATCH (c:Customer)-[:PLACED_ORDER]->(s:SalesOrder)
            WITH c, COUNT(s) as order_count, SUM(toFloat(s.totalNetAmount)) as total_value
            RETURN c.businessPartner as customer_id, 
                   c.businessPartnerName as customer_name,
                   order_count as number_of_orders, 
                   total_value as total_spent
            ORDER BY total_value DESC
            LIMIT 10
            """
        
        # Revenue
        if "revenue" in question_lower or "total" in question_lower and "sales" in question_lower:
            return """
            MATCH (b:BillingDocument)
            WHERE b.isCancelled = false
            RETURN SUM(toFloat(b.totalNetAmount)) as total_revenue,
                   COUNT(b) as total_invoices,
                   AVG(toFloat(b.totalNetAmount)) as average_invoice_value
            """
        
        # Trace flow for specific document
        if "trace" in question_lower or "flow" in question_lower:
            import re
            doc_match = re.search(r'\d{8}', question)
            doc_id = doc_match.group() if doc_match else "90504248"
            return f"""
            MATCH (b:BillingDocument {{billingDocument: '{doc_id}'}})
            OPTIONAL MATCH (b)-[:HAS_BILLING_ITEM]->(bi:BillingDocumentItem)
            OPTIONAL MATCH (bi)-[:BILLS_PRODUCT]->(p:Product)
            OPTIONAL MATCH (b)-[:GENERATES_JOURNAL_ENTRY]->(j:JournalEntry)
            OPTIONAL MATCH (b)-[:RECEIVES_PAYMENT]->(pay:Payment)
            OPTIONAL MATCH (c:Customer)-[:HAS_BILLING_DOCUMENT]->(b)
            RETURN b.billingDocument as billing_document,
                   bi.billingDocumentItem as billing_item,
                   p.product as product,
                   j.accountingDocument as journal_entry,
                   pay.accountingDocument as payment,
                   pay.clearingDate as payment_date,
                   c.businessPartner as customer
            """
        
        # Recent orders
        if "recent" in question_lower or "latest" in question_lower:
            return """
            MATCH (s:SalesOrder)
            RETURN s.salesOrder as sales_order, 
                   toFloat(s.totalNetAmount) as amount, 
                   s.creationDate as order_date
            ORDER BY s.creationDate DESC
            LIMIT 10
            """
        
        # Default
        return """
        MATCH (s:SalesOrder)
        RETURN s.salesOrder as sales_order, 
               toFloat(s.totalNetAmount) as amount, 
               s.creationDate as date
        ORDER BY s.creationDate DESC
        LIMIT 10
        """
    
    async def generate_response(self, question: str, data: List[Dict]) -> str:
        """Generate natural language response from query results"""
        if not data:
            return "📭 No results found for your query. Try rephrasing or checking the data."
        
        if self.use_mock:
            return self._mock_response_generation(question, data)
        
        try:
            result_count = len(data)
            data_sample = data[:10]
            data_text = json.dumps(data_sample, indent=2, default=str)
            
            prompt = f"""You are a business analyst. Generate a concise response.

Question: {question}
Number of results: {result_count}
Sample data:
{data_text}

Generate a clear, professional response with key findings."""
            
            response = self.client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "system", "content": "You are a business analyst providing data-driven answers."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=400
            )
            
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            print(f"Error generating response: {e}")
            return self._mock_response_generation(question, data)
    
    def _mock_response_generation(self, question: str, data: List[Dict]) -> str:
        """Fallback mock response generation"""
        if not data:
            return "📭 No results found."
        
        result_count = len(data)
        first_record = data[0]
        
        if 'product_id' in first_record:
            response = f"**🏆 Top Products by Billing Documents**\n\n"
            for i, record in enumerate(data[:5], 1):
                response += f"{i}. **{record['product_id']}** - {record['number_of_billing_documents']} documents (₹{float(record.get('total_billed_amount', 0)):,.2f})\n"
            return response
        
        if 'sales_order' in first_record and 'delivery_document' in first_record:
            response = f"**⚠️ Incomplete Orders (Delivered but not Billed)**\n\n"
            for i, record in enumerate(data[:5], 1):
                response += f"{i}. Order **{record['sales_order']}** - Delivered via {record['delivery_document']}\n"
            return response
        
        if 'customer_name' in first_record:
            response = f"**👥 Top Customers by Order Value**\n\n"
            for i, record in enumerate(data[:5], 1):
                response += f"{i}. **{record['customer_name']}** - {record['number_of_orders']} orders (₹{float(record.get('total_spent', 0)):,.2f})\n"
            return response
        
        if 'total_revenue' in first_record:
            return f"**💰 Total Revenue:** ₹{float(first_record['total_revenue']):,.2f}\n\n**Total Invoices:** {first_record['total_invoices']:,}\n**Average Invoice:** ₹{float(first_record.get('average_invoice_value', 0)):,.2f}"
        
        if 'billing_document' in first_record:
            response = f"**📋 Document Flow for {first_record['billing_document']}**\n\n"
            response += f"• **Customer:** {first_record.get('customer', 'N/A')}\n"
            response += f"• **Product:** {first_record.get('product', 'N/A')}\n"
            response += f"• **Journal Entry:** {first_record.get('journal_entry', 'N/A')}\n"
            response += f"• **Payment:** {first_record.get('payment', 'Not Processed')}\n"
            return response
        
        return f"Found {result_count} results. {json.dumps(data[:3], indent=2)}"