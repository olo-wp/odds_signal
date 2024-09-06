import datetime
from dataclasses import dataclass, asdict


@dataclass
class kurs:
    pierwszy: str = None
    drugi: str = None
    kurs1: float = None
    kursX: float = None
    kurs2: float = None
    data_meczu: datetime.datetime = None

def print_dom_variables(kurs_obj):
    print("gospodarz:", kurs_obj.pierwszy)
    print("gosc:", kurs_obj.drugi)
    print("1:", kurs_obj.kurs1)
    print("X:", kurs_obj.kursX)
    print("2:", kurs_obj.kurs2)
    print("data:", kurs_obj.data_meczu)

