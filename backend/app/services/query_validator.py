import re
from typing import Tuple, List

class QueryValidator:
    def __init__(self):
        self.dangerous_patterns = [
            r"DROP\s+",
            r"DELETE\s+",
            r"CREATE\s+",
            r"MERGE\s+",
            r"SET\s+.*=",
            r"REMOVE\s+",
            r"FOREACH\s+",
            r"APOC\.",
            r"gds\.",
            r"dbms\.",
        ]
        
        self.allowed_patterns = [
            r"MATCH\s+",
            r"OPTIONAL\s+MATCH\s+",
            r"WHERE\s+",
            r"RETURN\s+",
            r"WITH\s+",
            r"ORDER\s+BY\s+",
            r"LIMIT\s+\d+",
            r"SKIP\s+\d+",
            r"COUNT\s*\(",
            r"SUM\s*\(",
            r"AVG\s*\(",
            r"MIN\s*\(",
            r"MAX\s*\(",
            r"COLLECT\s*\(",
            r"DISTINCT\s+",
            r"AS\s+\w+",
            r"toFloat\s*\(",
            r"toString\s*\(",
            r"labels\s*\(",
            r"type\s*\(",
        ]
    
    def validate_cypher(self, query: str) -> Tuple[bool, str]:
        """Validate Cypher query for safety"""
        query_upper = query.upper()
        
        # Check for dangerous patterns
        for pattern in self.dangerous_patterns:
            if re.search(pattern, query_upper):
                return False, f"Query contains forbidden operation: {pattern}"
        
        # Check for basic structure
        if not re.search(r"MATCH", query_upper):
            return False, "Query must contain MATCH clause"
        
        if not re.search(r"RETURN", query_upper):
            return False, "Query must contain RETURN clause"
        
        # Check for reasonable length
        if len(query) > 2000:
            return False, "Query is too long (max 2000 characters)"
        
        return True, ""
    
    def sanitize_input(self, text: str) -> str:
        """Sanitize user input"""
        # Remove any potential injection patterns
        text = re.sub(r'["\']', '', text)
        text = re.sub(r'[;]', '', text)
        text = text.strip()
        return text[:500]  # Limit length

query_validator = QueryValidator()