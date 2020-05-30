import hashlib
import json
import os
import shutil
from io import BytesIO
from multiprocessing import Pool

import requests
import tqdm
from lxml import html
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import Base, Guild, Player

url = "https://www.florensia-online.com/de/rankings"
path = "./ranking.db"


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
    if not os.path.exists("./temp"):
        os.mkdir("./temp")

    # Download
    max_pages = get_number_of_pages()
    urls = [url + "?page=" + str(i) for i in range(1, max_pages+1)]

    pool = Pool(processes=6)
    for _ in tqdm.tqdm(pool.imap(download, urls),
                       total=len(urls),
                       desc="Download Status",
                       unit="Files"):
        pass

    # Database
    engine = create_engine(f"sqlite:///{path}", echo=False)

    try:
        Player.__table__.drop(bind=engine)
    except:
        pass
    finally:
        Base.metadata.create_all(bind=engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    guilds = set()
    players = []
    for index, fname in enumerate(os.listdir("./temp")):
        with open("./temp/" + fname) as f:
            data = json.load(f)

        players.extend([p for p in data if p["guild"] not in ["", None]])
        guilds.update([(p["guild"], p["server"]) for p in data if p["guild"] not in ["", None]])
        session.bulk_insert_mappings(Player, data)

    session.flush()

    guild_data = [{"id": index,
                   "name": guild[0],
                   "server": guild[1],
                   "name_hash": hashlib.md5(guild[0].encode()).hexdigest(),
                   } for index, guild in enumerate(guilds)
                  ]

    for guild in guild_data:
        guild_members = [p for p in players if p["guild"] == guild["name"]]
        guild["number_of_members"] = len(guild_members)
        guild["avg_rank"] = sum([p["rank"] for p in guild_members]) / len(guild_members)

    session.bulk_insert_mappings(Guild, guild_data)

    session.commit()

    shutil.rmtree("./temp")
