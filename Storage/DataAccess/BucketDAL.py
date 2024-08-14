from DataAccess.StorageManager import StorageManager
from DataAccess.ObjectManager import ObjectManager

class BucketDAL:
    def __init__(self): 

        self.storage_manager = StorageManager()
        self.object_manager = ObjectManager()


    def create(self) -> None:
        """create a new bucket."""
        #ליצור תקייה
        ObjectManager.create()
        #להכניס לקובץ json
        StorageManager.insert()
        return

    def insert(self) -> None:
        """Insert a new Object into managment file."""
        pass

    def update(self) -> None:
        """Update an existing object in managment file."""
        pass

    def get(self) -> None:
        """Get object from managment file."""
        pass
        

    def delete(self) -> None:
        """Delete an object from managment file."""
        pass