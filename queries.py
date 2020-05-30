from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from xlsxwriter import Workbook
from xlsxwriter.utility import xl_rowcol_to_cell

from models import Guild, Player

BASE_CLASSES = ["Noble", "Mercenary", "Saint", "Explorer"]

CC_CLASSES = [
    "Court Magician", "Magic Knight",
    "Guardian Swordsman", "Gladiator",
    "Excavator", "Sniper",
    "Priest", "Shaman"
]

ALL_CLASSES = [
    *BASE_CLASSES, *CC_CLASSES
]

LEVELS = list(range(1, 106))


if __name__ == "__main__":
    engine = create_engine("sqlite:///ranking.db", echo=False)

    Session = sessionmaker(bind=engine)
    session = Session()

    # any queries go here
    # e.g. session.query(Player).filter(Player.level == 105).count()
