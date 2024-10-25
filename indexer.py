import json
from http import HTTPStatus
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from loguru import logger

TOTAL_ITEMS = 14109
PAGE_SIZE = 500

CUR_DIR = Path(__file__).parent
INDEX_DIR = CUR_DIR / "index"
INDEX_DIR.mkdir(parents=True, exist_ok=True)


def main():
    page_count = (TOTAL_ITEMS + PAGE_SIZE - 1) // PAGE_SIZE
    for page_index in range(page_count):
        page_fpath = INDEX_DIR / f"{page_index}.jsonl"
        if page_fpath.is_file():
            logger.info(f"already done page {page_index}")
            continue

        logger.info(f"working on page {page_index}")
        link = f"https://www.cde.ca.gov/SchoolDirectory/districtschool?allsearch=elementary&simplesearch=Y&items={PAGE_SIZE}&page={page_index}&tab=1"
        resp = requests.get(url=link)
        if resp.status_code == HTTPStatus.OK:
            soup = BeautifulSoup(resp.text, "html.parser")

            rows = soup.select("tr")
            with page_fpath.open("w") as page_file:
                for row in rows:
                    if "class" not in row.attrs:
                        continue

                    cells = row.select("td")
                    keys = [
                        "cds_code",
                        "county",
                        "district",
                        "school",
                        "district_and_school_type",
                        "sector_type",
                        "charter",
                        "status",
                    ]
                    info = {}
                    for key, cell in zip(keys, cells):
                        info[key] = cell.text.strip()
                    info["link"] = row.select_one("a").attrs["href"]

                    page_file.write(json.dumps(info) + "\n")
        else:
            logger.error(f"request failed: {resp.status_code}")


if __name__ == "__main__":
    main()
