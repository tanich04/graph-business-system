import json
import asyncio
import os
from pathlib import Path
from typing import Dict, List, Any, Optional
import sys
from datetime import datetime

sys.path.append(str(Path(__file__).parent.parent))

from app.database.neo4j_client import neo4j_client

class CompleteDataIngestor:
    def __init__(self, data_path: str):
        self.data_path = Path(data_path)
        self.driver = None
        self.stats = {
            "nodes_created": {},
            "edges_created": {},
            "errors": []
        }
    
    def flatten_object(self, obj: Any, prefix: str = "") -> Dict[str, Any]:
        """Flatten nested objects into dot-notation properties"""
        if obj is None:
            return {}
        
        if not isinstance(obj, dict):
            return {prefix: obj} if prefix else {}
        
        flattened = {}
        for key, value in obj.items():
            new_key = f"{prefix}.{key}" if prefix else key
            
            if isinstance(value, dict):
                if "hours" in value and "minutes" in value:
                    flattened[new_key] = f"{value.get('hours', 0):02d}:{value.get('minutes', 0):02d}:{value.get('seconds', 0):02d}"
                else:
                    flattened.update(self.flatten_object(value, new_key))
            elif isinstance(value, list):
                flattened[new_key] = json.dumps(value)
            else:
                flattened[new_key] = value
        
        return flattened
    
    def transform_record(self, record: Dict) -> Dict:
        """Transform a record to be Neo4j compatible"""
        transformed = {}
        
        for key, value in record.items():
            if isinstance(value, dict):
                if "hours" in value and "minutes" in value:
                    transformed[key] = f"{value.get('hours', 0):02d}:{value.get('minutes', 0):02d}:{value.get('seconds', 0):02d}"
                else:
                    flattened = self.flatten_object(value, key)
                    transformed.update(flattened)
            elif isinstance(value, list):
                transformed[key] = json.dumps(value)
            else:
                transformed[key] = value
        
        return transformed
    
    def find_jsonl_files(self, folder_name: str) -> List[Path]:
        """Find all JSONL files in a specific folder"""
        folder_path = self.data_path / folder_name
        if not folder_path.exists():
            return []
        
        return list(folder_path.glob("*.jsonl"))
    
    async def load_jsonl_files(self, folder_name: str) -> List[Dict]:
        """Load all JSONL files from a folder and combine them"""
        all_records = []
        jsonl_files = self.find_jsonl_files(folder_name)
        
        for file_path in jsonl_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f, 1):
                        line = line.strip()
                        if line:
                            try:
                                record = json.loads(line)
                                transformed_record = self.transform_record(record)
                                all_records.append(transformed_record)
                            except json.JSONDecodeError as e:
                                print(f"  Error parsing line {line_num} in {file_path.name}: {e}")
                                continue
                print(f"    Loaded {len(all_records)} records from {file_path.name}")
            except Exception as e:
                print(f"    Error reading {file_path}: {e}")
        
        return all_records
    
    async def create_nodes_batch(self, session, label: str, records: List[Dict], 
                                  id_field: str = None, id_fields: List[str] = None):
        """Create nodes in batches with proper composite key handling"""
        if not records:
            return 0
        
        batch_size = 500
        total_created = 0
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            
            # Build dynamic MERGE query based on ID fields
            if id_fields and len(id_fields) > 1:
                # Composite key - build proper match pattern
                match_dict = ", ".join([f"{field}: record.{field}" for field in id_fields])
                query = f"""
                UNWIND $records AS record
                MERGE (n:{label} {{{match_dict}}})
                SET n += record
                """
            elif id_fields and len(id_fields) == 1:
                # Single key from list
                query = f"""
                UNWIND $records AS record
                MERGE (n:{label} {{ {id_fields[0]}: record.{id_fields[0]} }})
                SET n += record
                """
            elif id_field:
                # Single key from string
                query = f"""
                UNWIND $records AS record
                MERGE (n:{label} {{ {id_field}: record.{id_field} }})
                SET n += record
                """
            else:
                # No ID field - just create
                query = f"""
                UNWIND $records AS record
                CREATE (n:{label})
                SET n += record
                """
            
            try:
                await session.run(query, records=batch)
                total_created += len(batch)
                print(f"    Created {len(batch)} {label} nodes")
            except Exception as e:
                print(f"    Error creating {label} batch: {e}")
                self.stats["errors"].append(f"{label}: {str(e)}")
        
        return total_created
    
    async def create_edge_batch(self, session, edge_type: str, from_label: str, to_label: str,
                                 from_id_field: str, to_id_field: str, from_id_value: str,
                                 to_id_value: str, properties: Dict = None):
        """Create a single edge between nodes"""
        query = f"""
        MATCH (from:{from_label} {{ {from_id_field}: $from_id }})
        MATCH (to:{to_label} {{ {to_id_field}: $to_id }})
        MERGE (from)-[r:{edge_type}]->(to)
        """
        
        if properties:
            query += " SET r += $props"
        
        try:
            params = {
                "from_id": from_id_value,
                "to_id": to_id_value,
                "props": properties or {}
            }
            await session.run(query, **params)
            return 1
        except Exception as e:
            print(f"    Error creating edge {edge_type}: {e}")
            return 0
    
    async def ingest_billing_documents(self, session):
        """Ingest billing documents and related entities"""
        print("\n📄 Processing Billing Documents...")
        
        # 1. Billing Document Headers
        headers = await self.load_jsonl_files("billing_document_headers")
        if headers:
            created = await self.create_nodes_batch(session, "BillingDocument", headers, 
                                                     id_field="billingDocument")
            self.stats["nodes_created"]["BillingDocument"] = created
            
            # Create customer relationships
            for record in headers:
                if record.get("soldToParty"):
                    await self.create_edge_batch(session, "HAS_BILLING_DOCUMENT",
                                                   "Customer", "BillingDocument",
                                                   "businessPartner", "billingDocument",
                                                   record["soldToParty"], record["billingDocument"])
                    self.stats["edges_created"]["HAS_BILLING_DOCUMENT"] = \
                        self.stats["edges_created"].get("HAS_BILLING_DOCUMENT", 0) + 1
                
                if record.get("accountingDocument"):
                    await self.create_edge_batch(session, "GENERATES_JOURNAL_ENTRY",
                                                   "BillingDocument", "JournalEntry",
                                                   "billingDocument", "accountingDocument",
                                                   record["billingDocument"], record["accountingDocument"])
                    self.stats["edges_created"]["GENERATES_JOURNAL_ENTRY"] = \
                        self.stats["edges_created"].get("GENERATES_JOURNAL_ENTRY", 0) + 1
        
        # 2. Billing Document Items
        items = await self.load_jsonl_files("billing_document_items")
        if items:
            # Create nodes with composite key
            for record in items:
                record["item_id"] = f"{record['billingDocument']}_{record['billingDocumentItem']}"
            
            created = await self.create_nodes_batch(session, "BillingDocumentItem", items,
                                                     id_fields=["billingDocument", "billingDocumentItem"])
            self.stats["nodes_created"]["BillingDocumentItem"] = created
            
            for record in items:
                # HAS_BILLING_ITEM relationship
                await self.create_edge_batch(session, "HAS_BILLING_ITEM",
                                               "BillingDocument", "BillingDocumentItem",
                                               "billingDocument", "billingDocument",
                                               record["billingDocument"], record["billingDocument"])
                self.stats["edges_created"]["HAS_BILLING_ITEM"] = \
                    self.stats["edges_created"].get("HAS_BILLING_ITEM", 0) + 1
                
                # REFERENCES_PRODUCT relationship
                if record.get("material"):
                    await self.create_edge_batch(session, "REFERENCES_PRODUCT",
                                                   "BillingDocumentItem", "Product",
                                                   "billingDocumentItem", "product",
                                                   record["billingDocumentItem"], record["material"])
                    self.stats["edges_created"]["REFERENCES_PRODUCT"] = \
                        self.stats["edges_created"].get("REFERENCES_PRODUCT", 0) + 1
                
                # BILLS_ORDER_ITEM relationship
                if record.get("referenceSdDocument"):
                    await self.create_edge_batch(session, "BILLS_ORDER_ITEM",
                                                   "BillingDocumentItem", "SalesOrderItem",
                                                   "billingDocumentItem", "salesOrderItem",
                                                   record["billingDocumentItem"], 
                                                   f"{record['referenceSdDocument']}_{record['referenceSdDocumentItem']}")
                    self.stats["edges_created"]["BILLS_ORDER_ITEM"] = \
                        self.stats["edges_created"].get("BILLS_ORDER_ITEM", 0) + 1
                    
                    # BILLS_ORDER relationship
                    await self.create_edge_batch(session, "BILLS_ORDER",
                                                   "BillingDocument", "SalesOrder",
                                                   "billingDocument", "salesOrder",
                                                   record["billingDocument"], record["referenceSdDocument"])
                    self.stats["edges_created"]["BILLS_ORDER"] = \
                        self.stats["edges_created"].get("BILLS_ORDER", 0) + 1
        
        # 3. Billing Document Cancellations
        cancellations = await self.load_jsonl_files("billing_document_cancellations")
        if cancellations:
            for record in cancellations:
                if record.get("cancelledBillingDocument"):
                    await self.create_edge_batch(session, "CANCELLED_BY",
                                                   "BillingDocument", "BillingDocument",
                                                   "billingDocument", "billingDocument",
                                                   record["billingDocument"], record["cancelledBillingDocument"])
                    self.stats["edges_created"]["CANCELLED_BY"] = \
                        self.stats["edges_created"].get("CANCELLED_BY", 0) + 1
    
    async def ingest_sales_orders(self, session):
        """Ingest sales orders and related entities"""
        print("\n📦 Processing Sales Orders...")
        
        # 1. Sales Order Headers
        headers = await self.load_jsonl_files("sales_order_headers")
        if headers:
            created = await self.create_nodes_batch(session, "SalesOrder", headers, id_field="salesOrder")
            self.stats["nodes_created"]["SalesOrder"] = created
            
            for record in headers:
                if record.get("soldToParty"):
                    await self.create_edge_batch(session, "PLACED_ORDER",
                                                   "Customer", "SalesOrder",
                                                   "businessPartner", "salesOrder",
                                                   record["soldToParty"], record["salesOrder"])
                    self.stats["edges_created"]["PLACED_ORDER"] = \
                        self.stats["edges_created"].get("PLACED_ORDER", 0) + 1
        
        # 2. Sales Order Items
        items = await self.load_jsonl_files("sales_order_items")
        if items:
            # Create nodes with composite key
            for record in items:
                record["item_id"] = f"{record['salesOrder']}_{record['salesOrderItem']}"
            
            created = await self.create_nodes_batch(session, "SalesOrderItem", items,
                                                     id_fields=["salesOrder", "salesOrderItem"])
            self.stats["nodes_created"]["SalesOrderItem"] = created
            
            for record in items:
                # HAS_ORDER_ITEM relationship
                await self.create_edge_batch(session, "HAS_ORDER_ITEM",
                                               "SalesOrder", "SalesOrderItem",
                                               "salesOrder", "salesOrder",
                                               record["salesOrder"], record["salesOrder"])
                self.stats["edges_created"]["HAS_ORDER_ITEM"] = \
                    self.stats["edges_created"].get("HAS_ORDER_ITEM", 0) + 1
                
                # REFERENCES_PRODUCT relationship
                if record.get("material"):
                    await self.create_edge_batch(session, "REFERENCES_PRODUCT",
                                                   "SalesOrderItem", "Product",
                                                   "salesOrderItem", "product",
                                                   f"{record['salesOrder']}_{record['salesOrderItem']}", 
                                                   record["material"])
                    self.stats["edges_created"]["REFERENCES_PRODUCT"] = \
                        self.stats["edges_created"].get("REFERENCES_PRODUCT", 0) + 1
    
    async def ingest_deliveries(self, session):
        """Ingest deliveries and related entities"""
        print("\n🚚 Processing Deliveries...")
        
        # 1. Delivery Headers
        headers = await self.load_jsonl_files("outbound_delivery_headers")
        if headers:
            created = await self.create_nodes_batch(session, "Delivery", headers, id_field="deliveryDocument")
            self.stats["nodes_created"]["Delivery"] = created
        
        # 2. Delivery Items
        items = await self.load_jsonl_files("outbound_delivery_items")
        if items:
            # Create nodes with composite key
            for record in items:
                record["item_id"] = f"{record['deliveryDocument']}_{record['deliveryDocumentItem']}"
            
            created = await self.create_nodes_batch(session, "DeliveryItem", items,
                                                     id_fields=["deliveryDocument", "deliveryDocumentItem"])
            self.stats["nodes_created"]["DeliveryItem"] = created
            
            for record in items:
                # HAS_DELIVERY_ITEM relationship
                await self.create_edge_batch(session, "HAS_DELIVERY_ITEM",
                                               "Delivery", "DeliveryItem",
                                               "deliveryDocument", "deliveryDocument",
                                               record["deliveryDocument"], record["deliveryDocument"])
                self.stats["edges_created"]["HAS_DELIVERY_ITEM"] = \
                    self.stats["edges_created"].get("HAS_DELIVERY_ITEM", 0) + 1
                
                # FULFILLS_ORDER_ITEM relationship
                if record.get("referenceSdDocument"):
                    props = {"actualDeliveryQuantity": record.get("actualDeliveryQuantity")}
                    await self.create_edge_batch(session, "FULFILLS_ORDER_ITEM",
                                                   "DeliveryItem", "SalesOrderItem",
                                                   "deliveryDocumentItem", "salesOrderItem",
                                                   f"{record['deliveryDocument']}_{record['deliveryDocumentItem']}",
                                                   f"{record['referenceSdDocument']}_{record['referenceSdDocumentItem']}",
                                                   props)
                    self.stats["edges_created"]["FULFILLS_ORDER_ITEM"] = \
                        self.stats["edges_created"].get("FULFILLS_ORDER_ITEM", 0) + 1
                    
                    # SHIPPED_VIA relationship
                    await self.create_edge_batch(session, "SHIPPED_VIA",
                                                   "SalesOrder", "Delivery",
                                                   "salesOrder", "deliveryDocument",
                                                   record["referenceSdDocument"], record["deliveryDocument"])
                    self.stats["edges_created"]["SHIPPED_VIA"] = \
                        self.stats["edges_created"].get("SHIPPED_VIA", 0) + 1
    
    async def ingest_products(self, session):
        """Ingest products and related entities"""
        print("\n🏷️ Processing Products...")
        
        # 1. Products
        products = await self.load_jsonl_files("products")
        if products:
            created = await self.create_nodes_batch(session, "Product", products, id_field="product")
            self.stats["nodes_created"]["Product"] = created
        
        # 2. Product Descriptions
        descriptions = await self.load_jsonl_files("product_descriptions")
        if descriptions:
            for record in descriptions:
                query = """
                MATCH (p:Product {product: $product_id})
                SET p.productDescription = $description
                """
                await session.run(query, product_id=record["product"], 
                                description=record.get("productDescription", ""))
        
        # 3. Product Plants (STORED_AT relationship)
        product_plants = await self.load_jsonl_files("product_plants")
        if product_plants:
            for record in product_plants:
                props = {"mrpType": record.get("mrpType")}
                await self.create_edge_batch(session, "STORED_AT",
                                               "Product", "Plant",
                                               "product", "plant",
                                               record["product"], record["plant"],
                                               props)
                self.stats["edges_created"]["STORED_AT"] = \
                    self.stats["edges_created"].get("STORED_AT", 0) + 1
    
    async def ingest_customers(self, session):
        """Ingest customers and related entities"""
        print("\n👥 Processing Customers...")
        
        # 1. Business Partners (Customers)
        customers = await self.load_jsonl_files("business_partners")
        if customers:
            created = await self.create_nodes_batch(session, "Customer", customers, id_field="businessPartner")
            self.stats["nodes_created"]["Customer"] = created
        
        # 2. Business Partner Addresses
        addresses = await self.load_jsonl_files("business_partner_addresses")
        if addresses:
            created = await self.create_nodes_batch(session, "Address", addresses, id_field="addressId")
            self.stats["nodes_created"]["Address"] = created
            
            for record in addresses:
                if record.get("businessPartner"):
                    props = {
                        "validityStartDate": record.get("validityStartDate"),
                        "validityEndDate": record.get("validityEndDate")
                    }
                    await self.create_edge_batch(session, "HAS_ADDRESS",
                                                   "Customer", "Address",
                                                   "businessPartner", "addressId",
                                                   record["businessPartner"], record["addressId"],
                                                   props)
                    self.stats["edges_created"]["HAS_ADDRESS"] = \
                        self.stats["edges_created"].get("HAS_ADDRESS", 0) + 1
    
    async def ingest_plants(self, session):
        """Ingest plants and related entities"""
        print("\n🏭 Processing Plants...")
        
        plants = await self.load_jsonl_files("plants")
        if plants:
            created = await self.create_nodes_batch(session, "Plant", plants, id_field="plant")
            self.stats["nodes_created"]["Plant"] = created
            
            for record in plants:
                if record.get("addressId"):
                    await self.create_edge_batch(session, "LOCATED_AT",
                                                   "Plant", "Address",
                                                   "plant", "addressId",
                                                   record["plant"], record["addressId"])
                    self.stats["edges_created"]["LOCATED_AT"] = \
                        self.stats["edges_created"].get("LOCATED_AT", 0) + 1
    
    async def ingest_financials(self, session):
        """Ingest financial entities"""
        print("\n💰 Processing Financials...")
        
        # 1. Journal Entries
        journals = await self.load_jsonl_files("journal_entry_items_accounts_receivable")
        if journals:
            # Create nodes with composite key
            for record in journals:
                record["journal_id"] = f"{record['accountingDocument']}_{record['fiscalYear']}_{record['accountingDocumentItem']}"
            
            created = await self.create_nodes_batch(session, "JournalEntry", journals,
                                                     id_fields=["accountingDocument", "fiscalYear", "accountingDocumentItem"])
            self.stats["nodes_created"]["JournalEntry"] = created
        
        # 2. Payments
        payments = await self.load_jsonl_files("payments_accounts_receivable")
        if payments:
            # Create nodes with composite key
            for record in payments:
                record["payment_id"] = f"{record['accountingDocument']}_{record['accountingDocumentItem']}"
            
            created = await self.create_nodes_batch(session, "Payment", payments,
                                                     id_fields=["accountingDocument", "accountingDocumentItem"])
            self.stats["nodes_created"]["Payment"] = created
            
            for record in payments:
                if record.get("accountingDocument"):
                    await self.create_edge_batch(session, "RECEIVES_PAYMENT",
                                                   "BillingDocument", "Payment",
                                                   "accountingDocument", "accountingDocument",
                                                   record["accountingDocument"], record["accountingDocument"])
                    self.stats["edges_created"]["RECEIVES_PAYMENT"] = \
                        self.stats["edges_created"].get("RECEIVES_PAYMENT", 0) + 1
    
    async def ingest_all(self):
        """Main ingestion function"""
        print("=" * 80)
        print("🚀 Starting Complete Data Ingestion to Neo4j")
        print("=" * 80)
        
        await neo4j_client.connect()
        self.driver = neo4j_client.driver
        
        async with self.driver.session() as session:
            # Create constraints and indexes first
            print("\n📊 Creating constraints and indexes...")
            await neo4j_client.create_constraints()
            await neo4j_client.create_indexes()
            
            # Ingest all entities in order
            await self.ingest_customers(session)
            await self.ingest_plants(session)
            await self.ingest_products(session)
            await self.ingest_sales_orders(session)
            await self.ingest_deliveries(session)
            await self.ingest_billing_documents(session)
            await self.ingest_financials(session)
            
            # Print statistics
            print("\n" + "=" * 80)
            print("📈 INGESTION STATISTICS")
            print("=" * 80)
            print("\nNodes Created:")
            for node_type, count in sorted(self.stats["nodes_created"].items()):
                print(f"  {node_type}: {count}")
            
            print("\nEdges Created:")
            for edge_type, count in sorted(self.stats["edges_created"].items()):
                print(f"  {edge_type}: {count}")
            
            if self.stats["errors"]:
                print(f"\n⚠️ Errors: {len(self.stats['errors'])}")
                for error in self.stats["errors"][:5]:
                    print(f"  {error}")
            
            # Final verification
            print("\n" + "=" * 80)
            print("🔍 FINAL VERIFICATION")
            print("=" * 80)
            
            result = await session.run("""
                MATCH (n)
                RETURN labels(n)[0] as type, count(*) as count
                ORDER BY count DESC
            """)
            records = await result.data()
            for record in records:
                print(f"  {record['type']}: {record['count']}")
            
            result = await session.run("""
                MATCH ()-[r]->()
                RETURN type(r) as type, count(*) as count
                ORDER BY count DESC
                LIMIT 15
            """)
            records = await result.data()
            print("\n  Top Relationships:")
            for record in records:
                print(f"    {record['type']}: {record['count']}")
        
        await neo4j_client.close()
        print("\n✅✅✅ INGESTION COMPLETE! ✅✅✅")

async def main():
    DATASET_PATH = r"C:\Users\Tanishka\OneDrive\Documents\graph-business-system\backend\data"
    
    ingestor = CompleteDataIngestor(DATASET_PATH)
    await ingestor.ingest_all()

if __name__ == "__main__":
    asyncio.run(main())