import json
import os
from typing import Dict, List, Any, Optional
from tqdm import tqdm
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self, neo4j_client, data_dir: str):
        self.neo4j = neo4j_client
        self.data_dir = Path(data_dir)
        
    def get_all_jsonl_files(self) -> Dict[str, List[Path]]:
        """
        Scan directory structure and group JSONL files by folder name.
        Returns: {
            "billing_document_cancellations": [file1, file2, ...],
            "billing_document_headers": [file1, file2, ...],
            ...
        }
        """
        file_groups = {}
        
        # Iterate through all subdirectories
        for folder in self.data_dir.iterdir():
            if folder.is_dir():
                # Get all JSONL files in this folder
                jsonl_files = list(folder.glob("*.jsonl"))
                if jsonl_files:
                    file_groups[folder.name] = jsonl_files
                    logger.info(f"Found {len(jsonl_files)} files in {folder.name}")
        
        return file_groups
    
    def load_jsonl_file(self, filepath: Path) -> List[Dict]:
        """Load JSONL file and return list of records"""
        records = []
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    if line.strip():
                        try:
                            records.append(json.loads(line))
                        except json.JSONDecodeError as e:
                            logger.warning(f"JSON decode error in {filepath} line {line_num}: {e}")
                            continue
        except Exception as e:
            logger.error(f"Error reading {filepath}: {e}")
        
        return records
    
    def load_folder(self, folder_name: str, files: List[Path]) -> List[Dict]:
        """Load all files in a folder and combine records"""
        all_records = []
        for file in tqdm(files, desc=f"Loading {folder_name}"):
            records = self.load_jsonl_file(file)
            all_records.extend(records)
        logger.info(f"Loaded {len(all_records)} records from {folder_name}")
        return all_records
    
    def process_sales_orders(self, records: List[Dict]) -> List[Dict]:
        """Process sales order headers"""
        nodes = []
        for record in records:
            # Skip if no salesOrder
            if not record.get("salesOrder"):
                continue
                
            node = {
                "salesOrder": str(record.get("salesOrder", "")),
                "salesOrderType": record.get("salesOrderType", ""),
                "totalNetAmount": record.get("totalNetAmount", ""),
                "transactionCurrency": record.get("transactionCurrency", ""),
                "creationDate": record.get("creationDate", ""),
                "soldToParty": str(record.get("soldToParty", "")),
                "overallDeliveryStatus": record.get("overallDeliveryStatus", ""),
                "distributionChannel": record.get("distributionChannel", ""),
                "salesOrganization": record.get("salesOrganization", ""),
                "organizationDivision": record.get("organizationDivision", ""),
                "customerPaymentTerms": record.get("customerPaymentTerms", ""),
                "incotermsClassification": record.get("incotermsClassification", "")
            }
            nodes.append(node)
        return nodes
    
    def process_order_items(self, records: List[Dict]) -> List[Dict]:
        """Process order items"""
        nodes = []
        for record in records:
            if not record.get("salesOrder"):
                continue
                
            node = {
                "salesOrder": str(record.get("salesOrder", "")),
                "salesOrderItem": str(record.get("salesOrderItem", "")),
                "material": record.get("material", ""),
                "requestedQuantity": record.get("requestedQuantity", ""),
                "requestedQuantityUnit": record.get("requestedQuantityUnit", ""),
                "netAmount": record.get("netAmount", ""),
                "materialGroup": record.get("materialGroup", ""),
                "productionPlant": record.get("productionPlant", ""),
                "storageLocation": record.get("storageLocation", "")
            }
            nodes.append(node)
        return nodes
    
    def process_schedule_lines(self, records: List[Dict]) -> List[Dict]:
        """Process sales order schedule lines"""
        nodes = []
        for record in records:
            if not record.get("salesOrder"):
                continue
                
            node = {
                "salesOrder": str(record.get("salesOrder", "")),
                "salesOrderItem": str(record.get("salesOrderItem", "")),
                "scheduleLine": record.get("scheduleLine", ""),
                "confirmedDeliveryDate": record.get("confirmedDeliveryDate", ""),
                "orderQuantityUnit": record.get("orderQuantityUnit", ""),
                "confdOrderQtyByMatlAvailCheck": record.get("confdOrderQtyByMatlAvailCheck", "")
            }
            nodes.append(node)
        return nodes
    
    def process_deliveries(self, records: List[Dict]) -> List[Dict]:
        """Process delivery headers"""
        nodes = []
        for record in records:
            if not record.get("deliveryDocument"):
                continue
                
            node = {
                "deliveryDocument": str(record.get("deliveryDocument", "")),
                "shippingPoint": record.get("shippingPoint", ""),
                "overallGoodsMovementStatus": record.get("overallGoodsMovementStatus", ""),
                "creationDate": record.get("creationDate", ""),
                "actualGoodsMovementDate": record.get("actualGoodsMovementDate"),
                "deliveryBlockReason": record.get("deliveryBlockReason", ""),
                "headerBillingBlockReason": record.get("headerBillingBlockReason", "")
            }
            nodes.append(node)
        return nodes
    
    def process_delivery_items(self, records: List[Dict]) -> List[Dict]:
        """Process delivery items"""
        nodes = []
        for record in records:
            if not record.get("deliveryDocument"):
                continue
                
            node = {
                "deliveryDocument": str(record.get("deliveryDocument", "")),
                "deliveryDocumentItem": str(record.get("deliveryDocumentItem", "")),
                "actualDeliveryQuantity": record.get("actualDeliveryQuantity", ""),
                "plant": record.get("plant", ""),
                "referenceSdDocument": str(record.get("referenceSdDocument", "")),
                "referenceSdDocumentItem": str(record.get("referenceSdDocumentItem", "")),
                "batch": record.get("batch", ""),
                "storageLocation": record.get("storageLocation", "")
            }
            nodes.append(node)
        return nodes
    
    def process_billing_documents(self, records: List[Dict]) -> List[Dict]:
        """Process billing documents (both headers and cancellations)"""
        nodes = []
        for record in records:
            if not record.get("billingDocument"):
                continue
                
            node = {
                "billingDocument": str(record.get("billingDocument", "")),
                "billingDocumentType": record.get("billingDocumentType", ""),
                "totalNetAmount": record.get("totalNetAmount", ""),
                "billingDocumentDate": record.get("billingDocumentDate", ""),
                "billingDocumentIsCancelled": record.get("billingDocumentIsCancelled", False),
                "accountingDocument": record.get("accountingDocument", ""),
                "transactionCurrency": record.get("transactionCurrency", ""),
                "companyCode": record.get("companyCode", ""),
                "fiscalYear": record.get("fiscalYear", ""),
                "soldToParty": str(record.get("soldToParty", ""))
            }
            nodes.append(node)
        return nodes
    
    def process_billing_items(self, records: List[Dict]) -> List[Dict]:
        """Process billing items"""
        nodes = []
        for record in records:
            if not record.get("billingDocument"):
                continue
                
            node = {
                "billingDocument": str(record.get("billingDocument", "")),
                "billingDocumentItem": str(record.get("billingDocumentItem", "")),
                "material": record.get("material", ""),
                "billingQuantity": record.get("billingQuantity", ""),
                "billingQuantityUnit": record.get("billingQuantityUnit", ""),
                "netAmount": record.get("netAmount", ""),
                "transactionCurrency": record.get("transactionCurrency", ""),
                "referenceSdDocument": str(record.get("referenceSdDocument", "")),
                "referenceSdDocumentItem": str(record.get("referenceSdDocumentItem", ""))
            }
            nodes.append(node)
        return nodes
    
    def process_products(self, records: List[Dict]) -> List[Dict]:
        """Process products"""
        nodes = []
        for record in records:
            if not record.get("product"):
                continue
                
            node = {
                "product": record.get("product", ""),
                "productType": record.get("productType", ""),
                "creationDate": record.get("creationDate", ""),
                "lastChangeDate": record.get("lastChangeDate", ""),
                "isMarkedForDeletion": record.get("isMarkedForDeletion", False),
                "productOldId": record.get("productOldId", ""),
                "grossWeight": record.get("grossWeight", ""),
                "weightUnit": record.get("weightUnit", ""),
                "netWeight": record.get("netWeight", ""),
                "productGroup": record.get("productGroup", ""),
                "baseUnit": record.get("baseUnit", ""),
                "division": record.get("division", ""),
                "industrySector": record.get("industrySector", "")
            }
            nodes.append(node)
        return nodes
    
    def process_product_descriptions(self, records: List[Dict]) -> Dict[str, str]:
        """Process product descriptions - returns dict mapping product to description"""
        descriptions = {}
        for record in records:
            product = record.get("product")
            if product and record.get("productDescription"):
                descriptions[product] = record.get("productDescription")
        return descriptions
    
    def process_customers(self, records: List[Dict]) -> List[Dict]:
        """Process customers/business partners"""
        nodes = []
        for record in records:
            # Handle both business_partners and customer_company_assignments
            customer_id = str(record.get("customer", record.get("businessPartner", "")))
            if not customer_id:
                continue
                
            node = {
                "customer": customer_id,
                "businessPartner": str(record.get("businessPartner", "")),
                "businessPartnerName": record.get("businessPartnerName", record.get("organizationBpName1", "")),
                "businessPartnerFullName": record.get("businessPartnerFullName", ""),
                "businessPartnerCategory": record.get("businessPartnerCategory", ""),
                "businessPartnerGrouping": record.get("businessPartnerGrouping", ""),
                "correspondenceLanguage": record.get("correspondenceLanguage", ""),
                "createdByUser": record.get("createdByUser", ""),
                "creationDate": record.get("creationDate", ""),
                "businessPartnerIsBlocked": record.get("businessPartnerIsBlocked", False)
            }
            nodes.append(node)
        return nodes
    
    def process_addresses(self, records: List[Dict]) -> List[Dict]:
        """Process business partner addresses"""
        nodes = []
        for record in records:
            if not record.get("addressId"):
                continue
                
            node = {
                "addressId": str(record.get("addressId", "")),
                "businessPartner": str(record.get("businessPartner", "")),
                "cityName": record.get("cityName", ""),
                "country": record.get("country", ""),
                "postalCode": record.get("postalCode", ""),
                "streetName": record.get("streetName", ""),
                "region": record.get("region", ""),
                "addressTimeZone": record.get("addressTimeZone", "")
            }
            nodes.append(node)
        return nodes
    
    def process_plants(self, records: List[Dict]) -> List[Dict]:
        """Process plants"""
        nodes = []
        for record in records:
            if not record.get("plant"):
                continue
                
            node = {
                "plant": record.get("plant", ""),
                "plantName": record.get("plantName", ""),
                "valuationArea": record.get("valuationArea", ""),
                "plantCustomer": record.get("plantCustomer", ""),
                "plantSupplier": record.get("plantSupplier", ""),
                "factoryCalendar": record.get("factoryCalendar", ""),
                "salesOrganization": record.get("salesOrganization", ""),
                "addressId": record.get("addressId", ""),
                "distributionChannel": record.get("distributionChannel", "")
            }
            nodes.append(node)
        return nodes
    
    def process_journal_entries(self, records: List[Dict]) -> List[Dict]:
        """Process journal entry items"""
        nodes = []
        for record in records:
            if not record.get("accountingDocument"):
                continue
                
            node = {
                "accountingDocument": str(record.get("accountingDocument", "")),
                "fiscalYear": record.get("fiscalYear", ""),
                "companyCode": record.get("companyCode", ""),
                "glAccount": record.get("glAccount", ""),
                "referenceDocument": record.get("referenceDocument", ""),
                "profitCenter": record.get("profitCenter", ""),
                "amountInCompanyCodeCurrency": record.get("amountInCompanyCodeCurrency", ""),
                "transactionCurrency": record.get("transactionCurrency", ""),
                "amountInTransactionCurrency": record.get("amountInTransactionCurrency", ""),
                "postingDate": record.get("postingDate", ""),
                "documentDate": record.get("documentDate", ""),
                "customer": str(record.get("customer", "")),
                "financialAccountType": record.get("financialAccountType", "")
            }
            nodes.append(node)
        return nodes
    
    def process_customer_sales_areas(self, records: List[Dict]) -> List[Dict]:
        """Process customer sales area assignments"""
        nodes = []
        for record in records:
            if not record.get("customer"):
                continue
                
            node = {
                "customer": str(record.get("customer", "")),
                "salesOrganization": record.get("salesOrganization", ""),
                "distributionChannel": record.get("distributionChannel", ""),
                "division": record.get("division", ""),
                "currency": record.get("currency", ""),
                "customerPaymentTerms": record.get("customerPaymentTerms", ""),
                "deliveryPriority": record.get("deliveryPriority", ""),
                "incotermsClassification": record.get("incotermsClassification", ""),
                "incotermsLocation1": record.get("incotermsLocation1", ""),
                "shippingCondition": record.get("shippingCondition", ""),
                "supplyingPlant": record.get("supplyingPlant", "")
            }
            nodes.append(node)
        return nodes

    def process_payments(self, records: List[Dict]) -> List[Dict]:
        """Process payments"""
        nodes = []
        for record in records:
            if not record.get("accountingDocument"):
                continue
                
            node = {
                "accountingDocument": str(record.get("accountingDocument", "")),
                "accountingDocumentItem": record.get("accountingDocumentItem", ""),
                "clearingDate": record.get("clearingDate", ""),
                "clearingAccountingDocument": record.get("clearingAccountingDocument", ""),
                "clearingDocFiscalYear": record.get("clearingDocFiscalYear", ""),
                "amountInTransactionCurrency": record.get("amountInTransactionCurrency", ""),
                "transactionCurrency": record.get("transactionCurrency", ""),
                "amountInCompanyCodeCurrency": record.get("amountInCompanyCodeCurrency", ""),
                "customer": str(record.get("customer", "")),
                "postingDate": record.get("postingDate", ""),
                "glAccount": record.get("glAccount", "")
            }
            nodes.append(node)
        return nodes