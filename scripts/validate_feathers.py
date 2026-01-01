import argparse
import sys
import os
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed, Future
from typing import List, Tuple, Iterable, Optional

import polars as pl

from market_schema import (
    SCHEMA,
    validate_schema_shape,
    validate_session_type_values,
    check_string_normalization,
)

FEATHER_ROOT: Path = Path("marketnap/data")

# Type aliases
ValidationResult = Tuple[Path, bool, List[str]]


# --------------------------
# SINGLE FILE VALIDATION
# --------------------------
def validate_feather_file(path: Path, fix: bool = False) -> ValidationResult:
    try:
        df: pl.DataFrame = pl.read_ipc(path, memory_map=False)
    except Exception as e:
        return path, False, [f"Failed to read Feather: {e}"]

    errors: List[str] = []

    # Schema shape
    errors.extend(validate_schema_shape(df, SCHEMA))

    # Session type
    invalid_rows: List[int] = validate_session_type_values(df)
    if invalid_rows:
        errors.append(f"Invalid session_type at rows: {invalid_rows}")

    # String normalization
    string_issues = check_string_normalization(df)
    fixed: bool = False

    # Log errors first
    for col, issues in string_issues.items():
        for row, original, normalized in issues:
            errors.append(
                f"Column '{col}', row {row}: '{original}' → '{normalized}'"
            )

    # Apply fix **after logging all errors**
    if fix:
        for col, issues in string_issues.items():
            for row, _, normalized in issues:
                df = df.with_columns(
                    pl.when(pl.arange(0, pl.count()) == row)
                    .then(pl.lit(normalized))
                    .otherwise(pl.col(col))
                    .alias(col)
                )
                fixed = True

        if fixed:
            temp_path: Path = path.with_suffix(".tmp.feather")
            df.write_ipc(temp_path)
            if path.exists():
                path.unlink()
            temp_path.rename(path)
            print(f"[FIXED] {path}")

    return path, not errors, errors



# --------------------------
# PARALLEL RUNNER
# --------------------------
def validate_files_parallel(
    files: Iterable[Path],
    jobs: Optional[int] = None,
    fail_fast: bool = False,
    fix: bool = False,
) -> bool:
    max_workers: int = jobs if jobs is not None else (os.cpu_count() or 1)
    all_ok: bool = True

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures: List[Future[ValidationResult]] = [
            executor.submit(validate_feather_file, f, fix) for f in files
        ]

        for future in as_completed(futures):
            path, ok, errors = future.result()

            print(f"\nValidating {path}")
            if ok:
                print("[OK]")
            else:
                all_ok = False
                for err in errors:
                    print(f"[ERROR] {err}")
                if fail_fast:
                    print("\n[FAIL-FAST] Stopping.", file=sys.stderr)
                    return False

    return all_ok


# --------------------------
# CLI
# --------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate Feather files against schema"
    )
    parser.add_argument(
        "--all-files",
        action="store_true",
        help="Validate all Feather files (ignore git status)",
    )
    parser.add_argument(
        "--jobs",
        type=int,
        default=None,
        help="Number of parallel workers (default: CPU count)",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop on first failure",
    )
    parser.add_argument(
        "--fix",
        action="store_true",
        help="Automatically fix unnormalized strings in place",
    )

    args = parser.parse_args()

    feather_files: List[Path] = []

    if not args.all_files:
        try:
            import subprocess

            result = subprocess.run(
                ["git", "status", "--porcelain"],
                capture_output=True,
                text=True,
                check=True,
            )

            for line in result.stdout.splitlines():
                if not line:
                    continue
                p: Path = Path(line[3:])
                if p.suffix == ".feather":
                    feather_files.append(p)

        except Exception:
            print("[WARN] Git not available, falling back to --all-files")
            args.all_files = True

    if args.all_files:
        feather_files = list(FEATHER_ROOT.rglob("*.feather"))

    if not feather_files:
        print("No Feather files detected to validate.")
        return

    ok: bool = validate_files_parallel(
        feather_files,
        jobs=args.jobs,
        fail_fast=args.fail_fast,
        fix=args.fix,
    )

    if not ok:
        print("\n[FAIL] Some Feather files failed validation.", file=sys.stderr)
        sys.exit(1)

    print("\nAll Feather files passed validation.")


if __name__ == "__main__":
    main()
