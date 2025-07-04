"""
Convert a colon-delimited text file (~4 M rows) into four Excel workbooks
of up to 1 M rows each, keeping only selected columns.

USAGE (PowerShell or CMD)
------------------------
    python txttoexel.py path\to\input.txt

Dependencies (install once):
    python -m pip install --upgrade pandas xlsxwriter

The script streams the source file line-by-line, so RAM usage stays low.
"""

from __future__ import annotations

import csv
import re
import sys
from pathlib import Path
from typing import Iterable, List, Tuple

import pandas as pd

###############################################################################
# USER SETTINGS – adjust column indexes only if your file structure differs
###############################################################################
COL_SEP: str = ":"          # field delimiter in the source file
# Column indexes for the user's sample data:
PHONE_COL: int = 0           # phone number
FIRST_COL: int = 2           # first name
LAST_COL: int = 3            # last name
CITY_COL: int = 5            # city (first city field after gender)
CHUNK_LIMIT: int = 1_000_000 # rows per output workbook
OUTPUT_BASENAME: str = "output"  # produces output_1.xlsx … output_4.xlsx
###############################################################################

HEADER: List[str] = ["Phone", "FirstName", "LastName", "City"]


def parse_rows(fp) -> Iterable[Tuple[str, str, str, str]]:
    """Yield (phone, first, last, city) tuples from an open text file.

    Tries to repair lines that are missing the first one or two ':' delimiters
    (phone → id, id → first-name) by turning the first two whitespace runs into
    colons before normal CSV parsing.
    """

    for raw_line in fp:
        line = raw_line.rstrip("\n")

        # First attempt: naive split by ':'
        parts = line.split(COL_SEP)

        # If we don't yet have enough parts for the required column indexes,
        # fix missing delimiters at the start of the line (replace first 2
        # whitespace sequences with ':').
        if len(parts) <= max(PHONE_COL, FIRST_COL, LAST_COL, CITY_COL):
            fixed_line = re.sub(r"\s+", COL_SEP, line, count=2)
            parts = fixed_line.split(COL_SEP)

        # After the fix, validate again
        if len(parts) > max(PHONE_COL, FIRST_COL, LAST_COL, CITY_COL):
            # ------------------------------------------------------------------
            # LOCATION EXTRACTION
            # ------------------------------------------------------------------
            # In the raw line, fields ≥ index 5 may include one or two location
            # strings ("City, Country", sometimes with an intermediate region).
            # We skip known non-location tokens like gender and relationship
            # status and grab the first field that contains a comma.

            location_raw = ""
            skip_tokens = {
                "male", "female", "single", "married", "widowed",
                "divorced", "engaged", "in a relationship", "it's complicated",
            }

            for token in parts[5:]:
                value = token.strip()
                if not value:
                    continue
                ilower = value.lower()
                if ilower in skip_tokens:
                    continue
                # Skip dates like 09/01/1971 or 12/25
                if "/" in value:
                    continue
                # Skip tokens that are only digits (possibly with punctuation)
                if re.fullmatch(r"[0-9\-]+", value):
                    continue

                location_raw = value
                break

            # Transform "City, Country" or "City, Region, Country" to "Country: City"
            city_field = ""
            if location_raw:
                if "," in location_raw:
                    segments = [seg.strip() for seg in location_raw.split(",") if seg.strip()]
                    if segments:
                        city_segment = segments[0]
                        country_segment = segments[-1]
                        city_field = f"{country_segment}: {city_segment}"
                else:
                    city_field = location_raw

            yield (
                parts[PHONE_COL].strip(),
                parts[FIRST_COL].strip(),
                parts[LAST_COL].strip(),
                city_field,
            )
        # else: silently skip malformed lines (could alternatively log)


def write_chunk(rows: List[Tuple[str, str, str, str]], part_no: int) -> None:
    """Write a list of rows to an Excel file with the given part number."""
    out_name = f"{OUTPUT_BASENAME}_{part_no}.xlsx"
    df = pd.DataFrame(rows, columns=HEADER)
    # xlsxwriter is the default Excel writer for pandas when installed
    df.to_excel(out_name, index=False, engine="xlsxwriter")
    print(f"✓  wrote {len(rows):,} rows → {out_name}")


def process_txt(txt_path: Path) -> None:
    """Main streaming loop: read the txt file and write Excel chunks."""
    part_no = 1
    buffer: List[Tuple[str, str, str, str]] = []

    with txt_path.open("r", encoding="utf-8", newline="") as fp:
        for row in parse_rows(fp):
            buffer.append(row)
            if len(buffer) == CHUNK_LIMIT:
                write_chunk(buffer, part_no)
                part_no += 1
                buffer.clear()

    # Write any remainder (< 1M rows)
    if buffer:
        write_chunk(buffer, part_no)


if __name__ == "__main__":
    # ---------------------------------------------------------------------
    # Determine input file path
    # ---------------------------------------------------------------------
    if len(sys.argv) == 2:
        input_path = Path(sys.argv[1])
    else:
        # No argument: try to locate exactly one .txt file in CWD
        txt_files = list(Path.cwd().glob("*.txt"))
        if len(txt_files) == 1:
            input_path = txt_files[0]
            print(f"No path arg given -> using '{input_path.name}' in current directory.")
        else:
            print("Usage: python txttoexel.py path/to/input.txt")
            print("Or place exactly one .txt file in the current folder and run without arguments.")
            sys.exit(1)

    if not input_path.is_file():
        sys.exit(f"File not found: {input_path}")

    process_txt(input_path)