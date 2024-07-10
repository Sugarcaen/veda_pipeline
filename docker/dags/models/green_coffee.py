from decimal import Decimal
import hashlib
import json
import re
from typing import Union


class Score:
    def __init__(self, cupper_correction:int=0, score:int=None, **kwargs: Union[float, int]) -> None:
        self.__dict__.update(kwargs)
        self.cupper_correction = cupper_correction
        self.score = score or sum(self.__dict__.values())
        
    def to_dynamodb(self):
        tmp = self.__dict__
        return json.loads(json.dumps(tmp), parse_int=str, parse_float=str)

    def __repr__(self) -> str:
        return str(self.__dict__)

class FlavorWheel:
    def __init__(self, **kwargs) -> None:
        '''
        TODO: Think about nested flavors, Honey -> Brown Sugar -> Sweet
        Sweet Maria's wheel is static (at least it's overlaid on a standard image)
        '''
        self.__dict__.update(kwargs)
        
    def to_dynamodb(self):
        tmp = self.__dict__
        return json.loads(json.dumps(tmp), parse_int=str, parse_float=str)

    def __repr__(self) -> str:
        return str(self.__dict__)
           
class Farm:
    def __init__(self, location={"lat": "38.8977", "lng":"77.0365"}, description="No description Provided", is_direct_trade=False) -> None:
        self.location: dict = location
        self.description = description
        self.is_direct_trade = is_direct_trade

    def __repr__(self) -> str:
        return str(self.__dict__)

class GreenCoffeeProduct:
    re_size=re.compile(r"((?P<rangelow>\d+)\-(?P<rangehigh>\d+)\s*.*|(?P<grade>\d+)\+*)\s*[sS]+creen[\s\-]*(?P<cupper>.*)")
    re_defects=re.compile(r"(?P<defects>\.*\d+)[\s\+]+d/\d+gr")
    re_roasts=re.compile(r"^(?P<minroast>(([fF]ull\s)*[cC]ity\+*)|[cC]innamon)\s(to|-)\s(?P<maxroast>((([fF]ull\s)*[cC]ity\+*))|[fF]rench|[vV]ienna|[iI]talian|[sS]panish)")

    def __init__(self, farm: Farm, flavor: FlavorWheel, score: Score, prices: set=[], is_processed:bool=False, **kwargs) -> None:
        self.__dict__.update(kwargs)
        self.is_processed = is_processed
        self.farm = farm
        self.flavor = flavor
        self.score = score
        self.prices = prices
        if "appearance" in kwargs:
            appearance = kwargs.get("appearance")
            size=self.re_size.search(appearance)
            quality=self.re_defects.search(appearance)
            if size:
                self.size = size.group("grade") or (int(size.group("rangehigh")) + int(size.group("rangelow"))/2)
                self.appearance = size.group("cupper") or "no notes"
            if quality:
                self.defect_rate = quality.group("defects") if quality else "No Appearance"
            self.is_peaberry = "peaberry" in appearance.lower()
        if "roast_recommendations" in kwargs:
            roast_recommendations = kwargs.get("roast_recommendations")
            roast_rec=self.re_roasts.search(roast_recommendations)
            if roast_rec:
                self.min_roast = roast_rec.group("minroast")
                self.max_roast = roast_rec.group("maxroast")

    def to_dynamodb(self):
        tmp = self.__dict__
        tmp["id"] = hashlib.sha256(f"{tmp["seller"]}_{tmp["sku"]}".encode()).hexdigest()
        tmp["has_been_purchased"] = 0
        tmp["farm"] = tmp["farm"].__dict__
        tmp["flavor"] = tmp["flavor"].to_dynamodb()
        tmp["score"] = tmp["score"].to_dynamodb()
        tmp["prices"] = list(tmp["prices"])
        return json.loads(json.dumps(tmp), parse_float=Decimal)

    def __repr__(self) -> str:
        return str(self.__dict__)