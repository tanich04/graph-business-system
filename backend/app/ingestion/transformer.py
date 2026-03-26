# backend/app/ingestion/transformer_robust.py
import json
import logging
from typing import Dict, List, Any, Tuple, Optional

logger = logging.getLogger(__name__)

class RobustDataTransformer:
    """Robust transformer that handles missing fields gracefully"""
    
    def __init__(self):
        self.nodes = []
        self.relationships = []
    
    def safe_get(self, record: Dict, key: str, default=None):
        """Safely get value from record with logging"""
        if key not in record:
            logger.debug(f"Missing key '{key}' in record, using default: {default}")
            return default
        return record[key]
    
    def transform_billing_item(self, records: List[Dict]) -> Tuple[List, List]:
        """Robust billing item transformation"""
        nodes = []
        relationships = []
        
        for idx, record in enumerate(records):
            # Check if this is actually a billing record
            if 'billingDocument' not in record:
                logger.warning(f"Record {idx} missing billingDocument key, skipping: {record.get('_id', 'unknown')}")
                continue
            
            billing_doc = record.get('billingDocument')
            billing_item = record.get('billingDocumentItem', record.get('item', '1'))
            item_id = f"{billing_doc}_{billing_item}"
            
            # Create BillingItem node
            node = {
                "label": "BillingItem",
                "id": item_id,
                "properties": {
                    "id": item_id,
                    "billingDocument": billing_doc,
                    "itemNumber": billing_item,
                    "quantity": float(self.safe_get(record, 'billingQuantity', 0)),
                    "quantityUnit": self.safe_get(record, 'billingQuantityUnit', ''),
                    "netAmount": float(self.safe_get(record, 'netAmount', 0)),
                    "currency": self.safe_get(record, 'transactionCurrency', ''),
                    "material": self.safe_get(record, 'material', ''),
                    "referenceDocument": self.safe_get(record, 'referenceSdDocument', ''),
                }
            }
            nodes.append(node)
            
            # Relationship: BillingDocument -> BillingItem
            rel1 = {
                "type": "HAS_BILLING_ITEM",
                "from_label": "BillingDocument",
                "from_id": billing_doc,
                "to_label": "BillingItem",
                "to_id": item_id,
                "properties": {}
            }
            relationships.append(rel1)
            
            # Relationship: BillingItem -> Product
            material = self.safe_get(record, 'material', '')
            if material:
                rel2 = {
                    "type": "REFERENCES",
                    "from_label": "BillingItem",
                    "from_id": item_id,
                    "to_label": "Product",
                    "to_id": material,
                    "properties": {}
                }
                relationships.append(rel2)
            
            # Relationship: Order -> BillingDocument
            ref_doc = self.safe_get(record, 'referenceSdDocument', '')
            if ref_doc:
                rel3 = {
                    "type": "BILLED_AS",
                    "from_label": "Order",
                    "from_id": ref_doc,
                    "to_label": "BillingDocument",
                    "to_id": billing_doc,
                    "properties": {
                        "item": billing_item
                    }
                }
                relationships.append(rel3)
        
        logger.info(f"Transformed {len(nodes)} billing items")
        return nodes, relationships
    
    def transform_billing_header(self, records: List[Dict]) -> Tuple[List, List]:
        """Robust billing header transformation"""
        nodes = []
        relationships = []
        
        for record in records:
            if 'billingDocument' not in record:
                continue
            
            node = {
                "label": "BillingDocument",
                "id": record["billingDocument"],
                "properties": {
                    "id": record["billingDocument"],
                    "type": self.safe_get(record, 'billingDocumentType', ''),
                    "totalNetAmount": float(self.safe_get(record, 'totalNetAmount', 0)),
                    "currency": self.safe_get(record, 'transactionCurrency', ''),
                    "isCancelled": self.safe_get(record, 'billingDocumentIsCancelled', False),
                    "accountingDocument": self.safe_get(record, 'accountingDocument', ''),
                    "creationDate": self.safe_get(record, 'creationDate', ''),
                    "fiscalYear": self.safe_get(record, 'fiscalYear', ''),
                    "companyCode": self.safe_get(record, 'companyCode', ''),
                    "soldToParty": self.safe_get(record, 'soldToParty', ''),
                }
            }
            nodes.append(node)
            
            # Link to Customer
            if record.get('soldToParty'):
                rel = {
                    "type": "BILLS_TO",
                    "from_label": "BillingDocument",
                    "from_id": record["billingDocument"],
                    "to_label": "Customer",
                    "to_id": record["soldToParty"],
                    "properties": {}
                }
                relationships.append(rel)
        
        return nodes, relationships
    
    def transform_billing_cancellation(self, records: List[Dict]) -> Tuple[List, List]:
        """Handle billing cancellations"""
        relationships = []
        
        for record in records:
            if record.get('billingDocument') and record.get('cancelledBillingDocument'):
                rel = {
                    "type": "CANCELLED_BY",
                    "from_label": "BillingDocument",
                    "from_id": record["cancelledBillingDocument"],
                    "to_label": "BillingDocument",
                    "to_id": record["billingDocument"],
                    "properties": {
                        "cancellationDate": self.safe_get(record, 'creationDate', '')
                    }
                }
                relationships.append(rel)
        
        return [], relationships
    
    def transform_sales_order_item(self, records: List[Dict]) -> Tuple[List, List]:
        """Robust sales order item transformation"""
        nodes = []
        relationships = []
        
        for record in records:
            if 'salesOrder' not in record:
                continue
            
            item_id = f"{record['salesOrder']}_{self.safe_get(record, 'salesOrderItem', '1')}"
            
            node = {
                "label": "OrderItem",
                "id": item_id,
                "properties": {
                    "id": item_id,
                    "orderId": record["salesOrder"],
                    "itemNumber": self.safe_get(record, 'salesOrderItem', '1'),
                    "quantity": float(self.safe_get(record, 'requestedQuantity', 0)),
                    "quantityUnit": self.safe_get(record, 'requestedQuantityUnit', ''),
                    "netAmount": float(self.safe_get(record, 'netAmount', 0)),
                    "currency": self.safe_get(record, 'transactionCurrency', ''),
                    "materialGroup": self.safe_get(record, 'materialGroup', ''),
                    "material": self.safe_get(record, 'material', ''),
                }
            }
            nodes.append(node)
            
            # Link to Order
            rel1 = {
                "type": "HAS_ITEM",
                "from_label": "Order",
                "from_id": record["salesOrder"],
                "to_label": "OrderItem",
                "to_id": item_id,
                "properties": {}
            }
            relationships.append(rel1)
            
            # Link to Product
            if record.get('material'):
                rel2 = {
                    "type": "REFERENCES",
                    "from_label": "OrderItem",
                    "from_id": item_id,
                    "to_label": "Product",
                    "to_id": record["material"],
                    "properties": {}
                }
                relationships.append(rel2)
        
        return nodes, relationships
    
    def transform_delivery_item(self, records: List[Dict]) -> Tuple[List, List]:
        """Robust delivery item transformation"""
        nodes = []
        relationships = []
        
        for record in records:
            if 'deliveryDocument' not in record:
                continue
            
            item_id = f"{record['deliveryDocument']}_{self.safe_get(record, 'deliveryDocumentItem', '1')}"
            
            node = {
                "label": "DeliveryItem",
                "id": item_id,
                "properties": {
                    "id": item_id,
                    "deliveryDocument": record["deliveryDocument"],
                    "itemNumber": self.safe_get(record, 'deliveryDocumentItem', '1'),
                    "quantity": float(self.safe_get(record, 'actualDeliveryQuantity', 0)),
                    "quantityUnit": self.safe_get(record, 'deliveryQuantityUnit', ''),
                    "plant": self.safe_get(record, 'plant', ''),
                    "storageLocation": self.safe_get(record, 'storageLocation', ''),
                }
            }
            nodes.append(node)
            
            # Link to Delivery
            rel1 = {
                "type": "HAS_DELIVERY_ITEM",
                "from_label": "Delivery",
                "from_id": record["deliveryDocument"],
                "to_label": "DeliveryItem",
                "to_id": item_id,
                "properties": {}
            }
            relationships.append(rel1)
            
            # Link to Order if reference exists
            ref_doc = self.safe_get(record, 'referenceSdDocument', '')
            if ref_doc:
                rel2 = {
                    "type": "SHIPPED_VIA",
                    "from_label": "Order",
                    "from_id": ref_doc,
                    "to_label": "Delivery",
                    "to_id": record["deliveryDocument"],
                    "properties": {
                        "item": self.safe_get(record, 'referenceSdDocumentItem', '')
                    }
                }
                relationships.append(rel2)
        
        return nodes, relationships