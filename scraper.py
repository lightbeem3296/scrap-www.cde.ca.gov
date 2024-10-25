import json
import os
import sys
from http import HTTPStatus
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup
from loguru import logger
from slugify import slugify

CUR_DIR = Path(__file__).parent
OUTPUT_DIR = CUR_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def fetch(link: str) -> BeautifulSoup | None:
    ret = None
    resp = requests.get(
        url=link,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0",
        },
        timeout=15,
    )
    if resp.status_code == HTTPStatus.OK:
        ret = BeautifulSoup(resp.text, "html.parser")
    else:
        logger.error(f"request error: {resp.status_code}")
    return ret


def scrap(index: dict[str, str]) -> None:
    try:
        cds_code = index["cds_code"]

        file_name = f"{cds_code}.json"
        file_path = OUTPUT_DIR / file_name
        already_done = False
        if file_path.is_file():
            with file_path.open("r") as file:
                info: dict[str, Any] = json.load(file)
                if cds_code == info.get("cds_code", "#"):
                    already_done = True
        if already_done:
            logger.info(f"already done: {file_name}")
        else:
            logger.info(f"file: {file_name}")
            link = f"https://www.cde.ca.gov/SchoolDirectory/details?cdscode={cds_code}"
            soup = fetch(link=link)
            if isinstance(soup, BeautifulSoup):
                table = soup.select_one("table.table")
                rows = table.select("tr")

                info = index
                for row in rows:
                    if "hidden-print" == row.attrs.get("class", ""):
                        continue
                    try:
                        key = slugify(row.select_one("th").text.strip())
                        value = row.select_one("td").text.strip()
                        info[key] = value
                    except Exception as ex:
                        logger.exception(ex)

                with file_path.open("w") as file:
                    json.dump(info, file, indent=2, default=str)
            else:
                logger.error(f"failed fetch: {link}")
    except Exception as ex:
        logger.exception(ex)


def main():
    try:
        index_path_list: list[str] = []
        for arg in sys.argv:
            index_path = arg.strip()
            if os.path.isfile(index_path) and index_path.endswith(".jsonl"):
                index_path_list.append(index_path)

        for index_path in index_path_list:
            logger.info(f"index_file: {index_path}")
            with open(index_path, "r") as index_file:
                for line in index_file:
                    index = json.loads(line)
                    scrap(index)
    except Exception as ex:
        logger.exception(ex)


if __name__ == "__main__":
    main()
