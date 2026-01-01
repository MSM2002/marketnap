import argparse
from pathlib import Path
import polars as pl
import sys
import time
from typing import List

# ==========================
# DEFINE SCHEMA
# ==========================
SESSION_TYPE_VALUES: List[str] = ["Trading Holiday", "Settlement Holiday", "Special Session"]

# Use Polars DataType instances for type safety
SchemaType = dict[str, pl.DataType]
SCHEMA: SchemaType = {
    "date": pl.Date(),
    "description": pl.Utf8(),
    "session_type": pl.Categorical(),
    "circular_date": pl.Date(),
}

# --------------------------
# Helper: Clean string
# --------------------------
def clean_string(expr: pl.Expr) -> pl.Expr:
    """Strip leading/trailing spaces, collapse multiple spaces, title-case"""
    return expr.str.strip_chars().str.replace_all(r"\s+", " ").str.to_titlecase()

# --------------------------
# Validate 'session_type' column
# --------------------------
def validate_and_cast_session_type(df: pl.DataFrame, column: str = "session_type") -> pl.DataFrame:
    """Clean, validate all rows, log invalid, then cast to Categorical"""
    df = df.with_columns(clean_string(pl.col(column)).alias(column))
    valid_mask = df[column].is_in(SESSION_TYPE_VALUES)
    invalid_rows = df.filter(~valid_mask)
    if len(invalid_rows) > 0:
        print(f"\nERROR: {len(invalid_rows)} invalid rows in '{column}':")
        print(", ".join(invalid_rows[column].to_list()))
        raise ValueError(f"Validation failed: {len(invalid_rows)} invalid '{column}' values found.")
    return df.with_columns(pl.col(column).cast(pl.Categorical))

# --------------------------
# Align columns and cast types
# --------------------------
def align_and_cast(df: pl.DataFrame, schema: SchemaType) -> pl.DataFrame:
    missing = set(schema) - set(df.columns)
    extra = set(df.columns) - set(schema)
    if missing:
        raise ValueError(f"Missing columns in CSV: {sorted(missing)}")
    if extra:
        raise ValueError(f"Unexpected columns in CSV: {sorted(extra)}")

    exprs: List[pl.Expr] = []
    for name, dtype in schema.items():
        if name == "session_type":
            df = validate_and_cast_session_type(df, column=name)
            exprs.append(pl.col(name))
        else:
            exprs.append(pl.col(name).cast(dtype, strict=False).alias(name))
    return df.select(exprs)

# --------------------------
# Load CSV
# --------------------------
def load_csv(csv_path: Path) -> pl.DataFrame:
    return pl.read_csv(csv_path, infer_schema_length=0)

# --------------------------
# Validate existing Feather
# --------------------------
def validate_existing_feather(feather_path: Path, schema: SchemaType):
    if not feather_path.exists():
        return
    existing = pl.read_ipc(feather_path, memory_map=False)
    if dict(existing.schema) != schema:
        raise ValueError(
            "Existing Feather file schema does not match defined schema.\n"
            f"Expected: {schema}\nFound: {dict(existing.schema)}"
        )

# --------------------------
# Windows-safe Feather write
# --------------------------
def safe_write_ipc(df: pl.DataFrame, path: Path, retries: int = 3, delay: float = 1.0):
    """
    Write Feather safely on Windows using temp file + retry.
    """
    temp_path = path.with_suffix(".tmp.feather")
    for attempt in range(1, retries + 1):
        try:
            df.write_ipc(temp_path)
            # Atomic replace
            if path.exists():
                path.unlink()
            temp_path.rename(path)
            return
        except OSError as e:
            if getattr(e, "winerror", None) == 1224:
                print(f"File '{path}' is locked (open elsewhere). Attempt {attempt}/{retries}.")
                time.sleep(delay)
                continue
            raise
    raise RuntimeError(f"Failed to write '{path}' after {retries} attempts due to file lock.")

# --------------------------
# Append or overwrite
# --------------------------
def append_or_overwrite(csv_path: Path, feather_path: Path, mode: str):
    raw_df = load_csv(csv_path)
    casted_df = align_and_cast(raw_df, SCHEMA)
    validate_existing_feather(feather_path, SCHEMA)

    if mode == "overwrite" or not feather_path.exists():
        safe_write_ipc(casted_df, feather_path)
        print(f"Wrote {len(casted_df)} rows to {feather_path}")
        return

    # Append mode
    existing_df = pl.read_ipc(feather_path, memory_map=False)
    combined = pl.concat([existing_df, casted_df], how="vertical")
    safe_write_ipc(combined, feather_path)
    print(f"Appended {len(casted_df)} rows (total {len(combined)}) to {feather_path}")

# --------------------------
# CLI main
# --------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Append or overwrite a Feather file using a CSV (Polars, strict validation, Windows-safe)"
    )
    parser.add_argument("csv", type=Path, help="Input CSV file")
    parser.add_argument("feather", type=Path, help="Output Feather file")
    parser.add_argument(
        "--mode",
        choices=["append", "overwrite"],
        default="append",
        help="Write mode (default: append)"
    )

    args = parser.parse_args()
    try:
        append_or_overwrite(args.csv, args.feather, args.mode)
    except Exception as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
