"""
Graph Schema Definition
Maps relational entities to graph nodes and edges
"""

# Node Labels and their property keys
NODE_SCHEMA = {
    # Core Business Entities
    "BillingDocument": {
        "primary_key": "billingDocument",
        "properties": ["billingDocument", "billingDocumentType", "creationDate", 
                       "billingDocumentDate", "totalNetAmount", "transactionCurrency", 
                       "companyCode", "fiscalYear", "accountingDocument", "isCancelled"],
        "indexes": ["billingDocument", "accountingDocument", "soldToParty"]
    },
    
    "SalesOrder": {
        "primary_key": "salesOrder",
        "properties": ["salesOrder", "salesOrderType", "salesOrganization", 
                       "distributionChannel", "organizationDivision", "totalNetAmount", 
                       "transactionCurrency", "creationDate", "overallDeliveryStatus"],
        "indexes": ["salesOrder", "soldToParty"]
    },
    
    "Delivery": {
        "primary_key": "deliveryDocument",
        "properties": ["deliveryDocument", "shippingPoint", "creationDate", 
                       "overallGoodsMovementStatus", "actualGoodsMovementDate"],
        "indexes": ["deliveryDocument"]
    },
    
    "Product": {
        "primary_key": "product",
        "properties": ["product", "productType", "productGroup", "baseUnit", 
                       "grossWeight", "weightUnit"],
        "indexes": ["product"]
    },
    
    "Customer": {
        "primary_key": "businessPartner",
        "properties": ["businessPartner", "businessPartnerName", "businessPartnerCategory",
                       "organizationBpName1", "firstName", "lastName", "isBlocked"],
        "indexes": ["businessPartner"]
    },
    
    "Plant": {
        "primary_key": "plant",
        "properties": ["plant", "plantName", "salesOrganization", "addressId"],
        "indexes": ["plant"]
    },
    
    "Address": {
        "primary_key": "addressId",
        "properties": ["addressId", "cityName", "country", "postalCode", "streetName", "region"],
        "indexes": ["addressId"]
    },

    "CompanyCode": {
        "primary_key": "companyCode",
        "properties": ["companyCode"],
        "indexes": ["companyCode"]
    },

    "SalesArea": {
        "primary_key": "salesAreaId",
        "properties": ["salesAreaId", "salesOrganization", "distributionChannel", "division"],
        "indexes": ["salesAreaId"]
    },
    
    "JournalEntry": {
        "primary_key": "accountingDocument",
        "properties": ["accountingDocument", "fiscalYear", "companyCode", "postingDate"],
        "indexes": ["accountingDocument"]
    },
    
    "Payment": {
        "primary_key": "accountingDocument",
        "properties": ["accountingDocument", "clearingAccountingDocument", "clearingDate",
                       "amountInTransactionCurrency", "transactionCurrency"],
        "indexes": ["accountingDocument", "clearingAccountingDocument"]
    },
    
    # Line Items (stored as separate nodes for better graph traversal)
    "SalesOrderItem": {
        "primary_key": ["salesOrder", "salesOrderItem"],
        "properties": ["salesOrder", "salesOrderItem", "material", "requestedQuantity",
                       "netAmount", "productionPlant", "storageLocation"],
        "indexes": ["salesOrder", "material"]
    },
    
    "DeliveryItem": {
        "primary_key": ["deliveryDocument", "deliveryDocumentItem"],
        "properties": ["deliveryDocument", "deliveryDocumentItem", "actualDeliveryQuantity",
                       "deliveryQuantityUnit", "plant", "referenceSdDocument", "referenceSdDocumentItem"],
        "indexes": ["deliveryDocument", "referenceSdDocument"]
    },
    
    "BillingDocumentItem": {
        "primary_key": ["billingDocument", "billingDocumentItem"],
        "properties": ["billingDocument", "billingDocumentItem", "material", 
                       "billingQuantity", "netAmount", "referenceSdDocument", "referenceSdDocumentItem"],
        "indexes": ["billingDocument", "material", "referenceSdDocument"]
    },

    "SalesOrderScheduleLine": {
        "primary_key": "id",
        "properties": ["id", "salesOrder", "salesOrderItem", "scheduleLine", "confirmedDeliveryDate"],
        "indexes": ["salesOrder", "salesOrderItem"]
    },

    "CustomerCompanyAssignment": {
        "primary_key": "id",
        "properties": ["id", "customer", "companyCode", "paymentTerms", "reconciliationAccount"],
        "indexes": ["customer", "companyCode"]
    },

    "CustomerSalesAreaAssignment": {
        "primary_key": "id",
        "properties": ["id", "customer", "salesAreaId", "currency", "customerPaymentTerms"],
        "indexes": ["customer", "salesAreaId"]
    },

    "ProductPlant": {
        "primary_key": "id",
        "properties": ["id", "product", "plant", "mrpType", "availabilityCheckType", "profitCenter"],
        "indexes": ["product", "plant"]
    },

    "StorageLocation": {
        "primary_key": "storageLocationKey",
        "properties": ["storageLocationKey", "plant", "storageLocation"],
        "indexes": ["storageLocationKey", "plant"]
    },

    "ProductStorageLocation": {
        "primary_key": "id",
        "properties": ["id", "product", "plant", "storageLocation", "storageLocationKey"],
        "indexes": ["product", "storageLocationKey"]
    }
}

# Relationship Types and their directions
RELATIONSHIP_SCHEMA = {
    # Order Flow
    "HAS_ORDER_ITEM": {
        "from": "SalesOrder",
        "to": "SalesOrderItem",
        "properties": ["quantity", "netAmount"]
    },
    
    "REFERENCES_PRODUCT": {
        "from": "SalesOrderItem",
        "to": "Product",
        "properties": []
    },
    
    "SHIPPED_VIA": {
        "from": "SalesOrder",
        "to": "Delivery",
        "properties": []
    },
    
    "DELIVERS_ITEM": {
        "from": "Delivery",
        "to": "DeliveryItem",
        "properties": []
    },
    
    "FULFILLS_ORDER_ITEM": {
        "from": "DeliveryItem",
        "to": "SalesOrderItem",
        "properties": ["delivered_quantity"]
    },
    
    # Billing Flow
    "BILLS_ORDER": {
        "from": "BillingDocument",
        "to": "SalesOrder",
        "properties": []
    },
    
    "BILLS_ORDER_ITEM": {
        "from": "BillingDocumentItem",
        "to": "SalesOrderItem",
        "properties": ["billed_amount"]
    },
    
    "HAS_BILLING_ITEM": {
        "from": "BillingDocument",
        "to": "BillingDocumentItem",
        "properties": []
    },
    
    # Financial Flow
    "GENERATES_JOURNAL_ENTRY": {
        "from": "BillingDocument",
        "to": "JournalEntry",
        "properties": []
    },
    
    "RECEIVES_PAYMENT": {
        "from": "BillingDocument",
        "to": "Payment",
        "properties": []
    },
    
    "CANCELLED_BY": {
        "from": "BillingDocument",
        "to": "BillingDocument",
        "properties": ["cancellation_date"]
    },
    
    # Customer Relationships
    "PLACED_ORDER": {
        "from": "Customer",
        "to": "SalesOrder",
        "properties": []
    },
    
    "RECEIVES_DELIVERY": {
        "from": "Customer",
        "to": "Delivery",
        "properties": []
    },
    
    "HAS_BILLING_DOCUMENT": {
        "from": "Customer",
        "to": "BillingDocument",
        "properties": []
    },
    
    "HAS_ADDRESS": {
        "from": "Customer",
        "to": "Address",
        "properties": ["validity_start", "validity_end"]
    },

    "HAS_COMPANY_ASSIGNMENT": {
        "from": "Customer",
        "to": "CustomerCompanyAssignment",
        "properties": []
    },

    "FOR_COMPANY_CODE": {
        "from": "CustomerCompanyAssignment",
        "to": "CompanyCode",
        "properties": []
    },

    "HAS_SALES_AREA_ASSIGNMENT": {
        "from": "Customer",
        "to": "CustomerSalesAreaAssignment",
        "properties": []
    },

    "IN_SALES_AREA": {
        "from": "CustomerSalesAreaAssignment",
        "to": "SalesArea",
        "properties": []
    },
    
    # Location Relationships
    "LOCATED_AT": {
        "from": "Plant",
        "to": "Address",
        "properties": []
    },
    
    "STORED_AT": {
        "from": "Product",
        "to": "Plant",
        "properties": ["mrpType"]
    },

    "HAS_PLANT_DATA": {
        "from": "Product",
        "to": "ProductPlant",
        "properties": []
    },

    "AT_PLANT": {
        "from": "ProductPlant",
        "to": "Plant",
        "properties": []
    },

    "HAS_STORAGE_POSITION": {
        "from": "Product",
        "to": "ProductStorageLocation",
        "properties": []
    },

    "AT_STORAGE_LOCATION": {
        "from": "ProductStorageLocation",
        "to": "StorageLocation",
        "properties": []
    },

    "HAS_SCHEDULE_LINE": {
        "from": "SalesOrderItem",
        "to": "SalesOrderScheduleLine",
        "properties": []
    },

    "HAS_DESCRIPTION": {
        "from": "Product",
        "to": "ProductDescription",
        "properties": []
    },

    "IN_COMPANY_CODE": {
        "from": "BillingDocument",
        "to": "CompanyCode",
        "properties": []
    }
}

def get_node_label(entity_type: str) -> str:
    """Get Neo4j node label from entity type"""
    return entity_type

def get_relationship_type(rel_name: str) -> str:
    """Get Neo4j relationship type"""
    return rel_name
