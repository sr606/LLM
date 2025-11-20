"""
Minimal MCP-styled stub to expose the model for future integration.
"""
from typing import Dict, Any
 
class ModelProvider:
    def __init__(self, model: Dict[str, Any]):
        self.model = model
 
    def list_tables(self):
        return list(self.model.get("tables", {}).keys())
 
    def get_table(self, name: str) -> Dict[str, Any]:
        return self.model.get("tables", {}).get(name, {})
 
    def get_foreign_keys(self):
        return self.model.get("foreign_keys", [])