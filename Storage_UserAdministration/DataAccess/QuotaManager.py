import json
import os
from typing import Dict,  Optional
from Models.QuotaModel import QuotaModel

class QuotaManager:
    def __init__(self, file_path: str="D:/boto3 project/metadata.json"):
        self.file_path = file_path
        if not os.path.exists(self.file_path):
            with open(self.file_path, 'w') as file:
                json.dump({"server": {"policies": {}}, "users": {}}, file, indent=4)

    def load_data(self) -> Dict:
        """Load all data from the JSON file."""
        with open(self.file_path, 'r') as file:
            return json.load(file)

    def save_data(self, data: Dict):
        """Save all data to the JSON file."""
        with open(self.file_path, 'w') as file:
            json.dump(data, file, indent=4)

    def exists(self, quota_name: str) -> bool:
        """Check if a Quota exists."""
        data = self.load_data()
        return quota_name in data.get("server", {}).get("quotas", {})

    def insert(self, quota: QuotaModel):
        """Insert a new quota."""
        data = self.load_data()
        if quota.name in data["server"]["quotas"]:
            raise ValueError(f"Quota '{quota.name}' already exists.")
        data["server"]["quotas"][quota.name] = quota.to_dict()
        self.save_data(data)

    def delete(self, quota_name: str):
        """Delete an existing quota and remove references from users."""
        data = self.load_data()

        # Remove quota from "server" -> "quotas"
        if quota_name in data["server"]["quotas"]:
            del data["server"]["quotas"][quota_name]
        else:
            raise ValueError(f"Quota '{quota_name}' does not exist.")

        self.save_data(data)

    def update(self, quota: QuotaModel):
        """Update an existing quota."""
        data = self.load_data()
        if quota.name not in data["server"]["quotas"]:
            raise ValueError(f"Quota '{quota.name}' does not exist.")
        data["server"]["quotas"]= quota.to_dict()
        self.save_data(data)

    def select(self, name: str) -> Optional[QuotaModel]:
        """Select a quota by name."""
        data = self.load_data()
        quota_data = data.get("server", {}).get("quotas", {}).get(name)
        
        if quota_data:
            return QuotaModel(**quota_data)
        return None

    def list_all(self) -> Dict[str, QuotaModel]:
        """List all quotas."""
        data = self.load_data()
        quotas = {}
        for quota_name, quota_data in data.get("server", {}).get("quotas", {}).items():
            quotas[quota_name] = QuotaModel(
                name=quota_data['name'],
                resource_type=quota_data['resource_type'],
                restriction_type=quota_data['restriction_type'],
                limit=quota_data['limit'],
                period=quota_data['period'],
                usage=quota_data.get('usage', 0)
            )
        return quotas

