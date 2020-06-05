import asyncio
import json
import os
import shutil
import time
from io import BytesIO

import requests
from aiohttp import ClientSession, ClientTimeout
from lxml import html

from logger import setup_logger

URL = "https://www.florensia-online.com/de/rankings"
TEMP_PATH = "./temp"
logger = setup_logger()


def get_number_of_pages() -> int:
    content = requests.get(URL).content
    parser = html.parse(BytesIO(content))
    pagination_textes = parser.xpath(
        "//li[starts-with(@class, 'page-item')]/*/text()")
    # ['«', '1', '2', '3', '4', '3532', '3533', '»']
    return int(pagination_textes[-2])


async def fetch(url, session):
    try:
        async with session.get(url) as response:
            assert response.status == 200
            content = await response.read()
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
    except asyncio.exceptions.TimeoutError:
        page = url.split("?page=")[-1]
        logger.error(f"Failed to get page {page} - Timeout Error")


async def fetch_data():
    urls = [f"{URL}?page={i+1}" for i in range(get_number_of_pages())]
    logger.info(f"Found {len(urls)} url(s)")

    timeout = ClientTimeout(total=10*60)

    async with ClientSession(timeout=timeout) as session:
        tasks = [
            asyncio.ensure_future(fetch(url, session))
            for url in urls
        ]

        await asyncio.gather(*tasks)


def main():
    logger.info("-- Scrapper Script Start --")

    if not os.path.exists(TEMP_PATH):
        os.mkdir(TEMP_PATH)
        
    # Fetching data
    logger.info("Start fetching data")
    start_time = time.time()

    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(fetch_data())
    loop.run_until_complete(future)

    logger.info(f"Finished fetching data in {round(time.time() - start_time, 2)} seconds")

    # Read data
    players = []
    for f in os.listdir(TEMP_PATH):
        with open(os.path.join(TEMP_PATH, f), "r") as fp:
            players.extend(json.load(fp))

    with open("players.json", "w") as fp:
        json.dump(players, fp)

    shutil.rmtree(TEMP_PATH)

    logger.info("-- Scrapper Script End --")


if __name__ == "__main__":
    main()
