from typing import Dict, List
import polars as pl

# ==========================
# DOMAIN CONFIG
# ==========================
SESSION_TYPE_VALUES: List[str] = [
    "Trading Holiday",
    "Settlement Holiday",
    "Special Session",
]

SchemaType = Dict[str, pl.DataType]

SCHEMA: SchemaType = {
    "date": pl.Date(),
    "description": pl.Utf8(),
    "session_type": pl.Categorical(),
    "circular_date": pl.Date(),
}

# Columns that must be normalized and their expected final dtype
NORMALIZED_STRING_COLUMNS: Dict[str, pl.DataType] = {
    "description": pl.Utf8(),
    "session_type": pl.Categorical(),
}

# ==========================
# STRING NORMALIZATION
# ==========================
def normalize_string_expr(expr: pl.Expr) -> pl.Expr:
    """Normalization used during ingest (Expr-based)."""
    return (
        expr
        .str.strip_chars()
        .str.replace_all(r"\s+", " ")
        .str.to_titlecase()
    )


def normalize_string_series(series: pl.Series) -> pl.Series:
    """
    Normalize a Series safely.
    If categorical, temporarily cast to Utf8.
    """
    if series.dtype == pl.Categorical:
        series = series.cast(pl.Utf8)

    return (
        series
        .str.strip_chars()
        .str.replace_all(r"\s+", " ")
        .str.to_titlecase()
    )

# ==========================
# ROW INDEX HELPERS
# ==========================
def rows_where(mask: pl.Series) -> List[int]:
    """Return row indices where mask is True."""
    return (
        pl.DataFrame({"mask": mask})
        .with_row_index("row")
        .filter(pl.col("mask"))
        .select("row")
        .to_series()
        .to_list()
    )

# ==========================
# VALIDATION HELPERS
# ==========================
def validate_schema_shape(df: pl.DataFrame, schema: SchemaType) -> List[str]:
    errors: List[str] = []

    missing = set(schema) - set(df.columns)
    if missing:
        errors.append(f"Missing columns: {sorted(missing)}")

    extra = set(df.columns) - set(schema)
    if extra:
        errors.append(f"Unexpected columns: {sorted(extra)}")

    for col, dtype in schema.items():
        if col in df.columns:
            try:
                df.select(pl.col(col).cast(dtype, strict=False))
            except Exception as e:
                errors.append(f"Column '{col}' cannot be cast to {dtype}: {e}")

    return errors


def validate_session_type_values(
    df: pl.DataFrame,
    column: str = "session_type",
) -> List[int]:
    if column not in df.columns:
        return []

    series = df[column]
    if series.dtype == pl.Categorical:
        series = series.cast(pl.Utf8)

    mask = ~series.is_in(SESSION_TYPE_VALUES)
    return rows_where(mask)


from typing import Dict, List, Tuple

def check_string_normalization(
    df: pl.DataFrame,
) -> Dict[str, List[Tuple[int, str, str]]]:
    """
    Returns:
        column -> [(row_index, original_value, normalized_value)]
    """
    issues: Dict[str, List[Tuple[int, str, str]]] = {}

    for col in NORMALIZED_STRING_COLUMNS.keys():
        if col not in df.columns:
            continue

        series = df[col]

        # Work in string space
        original = (
            series.cast(pl.Utf8)
            if series.dtype == pl.Categorical
            else series
        )

        normalized = normalize_string_series(series)
        mask = original != normalized

        if not mask.any():
            continue

        rows = rows_where(mask)

        bad_values = [
            (
                row,
                original[row],
                normalized[row],
            )
            for row in rows
        ]

        issues[col] = bad_values

    return issues


# ==========================
# TRANSFORMATION (INGEST)
# ==========================
def align_and_cast(df: pl.DataFrame, schema: SchemaType) -> pl.DataFrame:
    missing = set(schema) - set(df.columns)
    extra = set(df.columns) - set(schema)

    if missing:
        raise ValueError(f"Missing columns in CSV: {sorted(missing)}")
    if extra:
        raise ValueError(f"Unexpected columns in CSV: {sorted(extra)}")

    exprs: List[pl.Expr] = []

    for name, dtype in schema.items():
        expr = pl.col(name)

        if name in NORMALIZED_STRING_COLUMNS:
            expr = normalize_string_expr(expr)

        if name == "session_type":
            df = df.with_columns(expr.alias(name))
            invalid = validate_session_type_values(df, name)
            if invalid:
                bad = (
                    df
                    .with_row_index("row")
                    .filter(pl.col("row").is_in(invalid))
                    .select(name)
                    .to_series()
                    .to_list()
                )
                raise ValueError(f"Invalid session_type values: {bad}")

            exprs.append(pl.col(name).cast(pl.Categorical))
        else:
            exprs.append(expr.cast(dtype, strict=False).alias(name))

    return df.select(exprs)
