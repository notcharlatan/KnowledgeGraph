# src/models.py
from dataclasses import dataclass
from typing import List, Dict, Any


@dataclass
class Company:
    company_name: str
    establish_year: int
    headquarter: str
    company_type: str
    fleet_size: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "companyName": self.company_name,
            "establishYear": self.establish_year,
            "headquarter": self.headquarter,
            "companyType": self.company_type,
            "fleetSize": self.fleet_size
        }


@dataclass
class Ship:
    ship_name: str
    ship_type: str
    deadweight: int
    length: float
    speed: float
    build_year: int
    draft: float
    company_name: str  # 所属公司名称，用于建立关系

    def to_dict(self) -> Dict[str, Any]:
        return {
            "shipName": self.ship_name,
            "shipType": self.ship_type,
            "deadweight": self.deadweight,
            "length": self.length,
            "speed": self.speed,
            "buildYear": self.build_year,
            "draft": self.draft
        }