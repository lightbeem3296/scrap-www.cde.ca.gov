import json
import os
import time
from pathlib import Path

import pandas as pd
from loguru import logger

CUR_DIR = Path(__file__).parent
OUTPUT_DIR = CUR_DIR / "output"


def regulate_str(input_str: str) -> str:
    lines = input_str.splitlines()

    trimmed_lines = [line.strip() for line in lines]

    regulated_lines = []
    previous_line_empty = False

    for line in trimmed_lines:
        if line == "":
            if not previous_line_empty:
                regulated_lines.append(line)
            previous_line_empty = True
        else:
            regulated_lines.append(line)
            previous_line_empty = False

    ret = "\n".join(regulated_lines)
    return ret.strip()


def merge():
    try:
        dst_fpath = CUR_DIR / f"output_{time.time()}.csv"

        res_df = pd.DataFrame()

        logger.info(f"[*] merge into > {dst_fpath}")
        file_number = 0
        for dpath, _, fnames in os.walk(OUTPUT_DIR):
            for fname in fnames:
                if not fname.lower().endswith(".json"):
                    continue

                fpath = os.path.join(dpath, fname)
                logger.info(f"{file_number}: {fname}")
                with open(fpath, mode="r") as f:
                    info: dict[str, str] = json.load(f)
                    store_info = {}
                    for key, value in info.items():
                        if key == "link":
                            store_info["link"] = "https://www.cde.ca.gov" + value
                        elif key in ["statistical-info"]:
                            continue
                        elif key in ["school-address", "web-address"]:
                            store_info[key] = regulate_str(
                                value.replace(
                                    "Link opens new browser tab",
                                    "",
                                ).replace(
                                    "Google Map",
                                    "",
                                )
                            )
                        elif key in ["email"]:
                            store_info[key] = regulate_str(
                                value.replace("Link opens new Email", "")
                            )
                        else:
                            store_info[key] = regulate_str(value)

                    res_df = pd.concat(
                        [res_df, pd.DataFrame([store_info])],
                        ignore_index=True,
                    )
                file_number += 1
        res_df.to_csv(dst_fpath, index=False)

    except Exception as ex:
        logger.exception(ex)


def main():
    try:
        merge()
    except Exception as ex:
        logger.exception(ex)


if __name__ == "__main__":
    main()
