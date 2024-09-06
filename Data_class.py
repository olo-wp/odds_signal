import datetime
from dataclasses import dataclass, asdict


@dataclass
class odds:
    game: str = None
    home: float = None
    draw: float = None
    away: float = None
    date: datetime.datetime = None

def print_dom_variables(odds_obj):
    print("game:", odds_obj.game)
    print("1:", odds_obj.home)
    print("X:", odds_obj.draw)
    print("2:", odds_obj.away)
    print("date:", odds_obj.date)

