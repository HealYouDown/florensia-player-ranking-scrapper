import requests
from io import BytesIO
from lxml import html
from multiprocessing.pool import ThreadPool
import time
import json
import os
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base

url = "https://www.florensia-online.com/de/rankings"
Base = declarative_base()


class Player(Base):
    __tablename__ = "player"
    rank = Column(Integer, primary_key=True)
    name = Column(String)
    class_ = Column(String)
    level_land = Column(Integer)
    level_sea = Column(Integer)
    guild = Column(String)
    server = Column(String)


def get_number_of_pages() -> int:
    content = requests.get(url).content
    parser = html.parse(BytesIO(content))
    pagination_textes = parser.xpath(
        "//li[starts-with(@class, 'page-item')]/*/text()")
    # ['«', '1', '2', '3', '4', '3532', '3533', '»']
    return int(pagination_textes[-2])


def download(url: str):
    content = requests.get(url).content
    
    parser = html.parse(BytesIO(content))

    table_data_elements = parser.xpath("//tbody/tr/td")
    table_data = [elem.text for elem in table_data_elements]

    players = []

    for row in zip(*[iter(table_data)] * 10):
        players.append({
            "rank": int(row[0]),
            "name": row[2],
            "class_": row[3],
            "level_land": int(row[4]),
            "level_sea": int(row[6]),
            "guild": row[8],
            "server": row[9]
        })

    fname = url.split("=")[1]
    with open(f"./temp/{fname}.json", "w") as f:
        json.dump(players, f)


if __name__ == "__main__":
    # Download
    t1 = time.time()

    max_pages = get_number_of_pages()
    urls = [url + "?page=" + str(i) for i in range(1, max_pages+1)]

    ThreadPool(32).map(download, urls)

    print(f"Finished downloading in {time.time() - t1}")

    # Database
    engine = create_engine("sqlite:///ranking.db", echo=False)

    try:
        Player.__table__.drop(bind=engine)
    except:
        pass
    finally:
        Base.metadata.create_all(bind=engine)
    
    Session = sessionmaker(bind=engine)
    session = Session()
    
    max_length = len(os.listdir("./temp"))
    for index, fname in enumerate(os.listdir("./temp")):
        with open("./temp/" + fname) as f:
            data = json.load(f)

        session.bulk_insert_mappings(Player, data)

    session.commit()

    print(f"Finished in {time.time() - t1}")
