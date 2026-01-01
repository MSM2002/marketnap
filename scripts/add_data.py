import argparse
import sys
import time
from pathlib import Path
import polars as pl

from market_schema import SCHEMA, align_and_cast

# --------------------------
# IO HELPERS
# --------------------------
def load_csv(csv_path: Path) -> pl.DataFrame:
    return pl.read_csv(csv_path, infer_schema_length=0)


def validate_existing_feather(feather_path: Path):
    if not feather_path.exists():
        return
    existing = pl.read_ipc(feather_path, memory_map=False)
    if dict(existing.schema) != SCHEMA:
        raise ValueError("Existing Feather schema does not match expected schema")


def safe_write_ipc(df: pl.DataFrame, path: Path, retries: int=3, delay: float=1.0):
    temp = path.with_suffix(".tmp.feather")
    for attempt in range(1, retries + 1):
        try:    
            df.write_ipc(temp)
            if path.exists():
                path.unlink()
            temp.rename(path)
            return
        except OSError as e:
            if getattr(e, "winerror", None) == 1224:
                print(f"File locked, retry {attempt}/{retries}")
                time.sleep(delay)
                continue
            raise
    raise RuntimeError(f"Failed to write {path} after {retries} attempts")

# --------------------------
# CORE LOGIC
# --------------------------
def append_or_overwrite(csv: Path, feather: Path, mode: str):
    raw = load_csv(csv)
    df = align_and_cast(raw, SCHEMA)
    validate_existing_feather(feather)

    if mode == "overwrite" or not feather.exists():
        safe_write_ipc(df, feather)
        print(f"Wrote {len(df)} rows to {feather}")
        return

    existing = pl.read_ipc(feather, memory_map=False)
    combined = pl.concat([existing, df], how="vertical")
    safe_write_ipc(combined, feather)
    print(f"Appended {len(df)} rows (total {len(combined)}) to {feather}")

# --------------------------
# CLI
# --------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Append or overwrite a Feather file from CSV"
    )
    parser.add_argument("csv", type=Path)
    parser.add_argument("feather", type=Path)
    parser.add_argument(
        "--mode",
        choices=["append", "overwrite"],
        default="append",
    )
    args = parser.parse_args()

    try:
        append_or_overwrite(args.csv, args.feather, args.mode)
    except Exception as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
