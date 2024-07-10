import decimal
import hashlib
import json

class RoastedCoffee:
    def __init__(self, name:str, price:str, roast_level:str, green_key:str) -> None:
        self.name = name
        self.price = price
        self.roast_level = roast_level
        self.green_key = green_key
    
    def to_dynamo(self):
        tmp = self.__dict__
        tmp["id"] = hashlib.sha256(f"{tmp["name"]}_{tmp["roast_level"]}".encode()).hexdigest()
        return json.loads(json.dumps(tmp))

    
