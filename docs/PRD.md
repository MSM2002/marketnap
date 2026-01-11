# MarketNap – Project Requirements & Design Notes

## 1. Project Overview

MarketNap is a Python library that provides market calendars for Indian capital markets, starting with NSE and BSE.

The library focuses on:
- Trading days
- Market holidays
- Special trading sessions
- Fast calendar generation
- Offline usage (calendar data is offline, but version checks can be online)

The project is designed to grow organically to support:
- More exchanges
- More market segments
- Additional calendar logic

---

## 2. Core Design Principles

- Offline-first: No network calls for calendar generation
- Data updates happen only via PyPI package upgrades
- Lightweight installation
- Fast execution
- Explicit and transparent versioning
- Minimal online activity limited to version checks only

---

## 3. Supported Markets (Initial Scope)

- NSE (India)
- BSE (India)

Each exchange may have multiple market segments.

---

## 4. Data Storage

### 4.1 Embedded Package Data

- Market holiday and special session data is stored in Feather files
- Feather files are bundled directly inside the Python package
- Data is sharded by exchange and market segment

Example structure:

    marketnap/data/
    ├─ nse/
    │  ├─ equity.feather
    │  ├─ fo.feather
    │  └─ currency.feather
    └─ bse/
       ├─ equity.feather
       └─ fo.feather

- Each Feather file represents a single exchange–segment combination

---

### 4.2 Feather File Schema

The schema for each Feather file is designed to be simple, programmatically friendly, and future-proof:

| Column        | Description |
|---------------|-------------|
| `date`        | The calendar date of the holiday or special session |
| `description` | Human-readable description of the event (e.g., “Republic Day”, “Early Close”) |
| `session_type`| Categorizes the day, e.g., `Trading Holiday`, `Settlement Holiday`, `Special Session` |
| `circular_date`| The official exchange circular date that announced the holiday or session |

**Notes:**
- Columns are lowercase and snake_case for consistency with Python code.
- Special session timings (`start_time` / `end_time`) are **not included initially** due to complexity and unreliable historical data, but can be added in the future as optional nullable columns.
- This schema allows easy filtering, versioning, and calendar generation.

---

## 5. Data Backend

- Polars is used as the primary dataframe backend
- Chosen for speed, low memory usage, and native Feather support
- Pandas may be added later as an optional backend

---

## 6. Calendar Generation Logic

- Feather files store only:
  - Holidays
  - Special trading sessions
- Full market calendars are generated dynamically by:
  - Excluding weekends
  - Excluding holidays
  - Including special sessions where applicable
- Calendar generation operates on a single exchange–segment dataset at a time

---

## 7. Versioning Strategy

### 7.1 Package Version

- Uses semantic versioning: major.minor.patch
- Version is stored in:
  - pyproject.toml
  - marketnap/version.py

Example:

    __version__ = "1.0.3"

Version bump rules:
- Patch: bug fixes or data updates
- Minor: new features or new exchanges or segments (non-breaking)
- Major: breaking API changes

---

### 7.2 Data Versioning (Per Exchange–Segment)

- Each exchange–segment pair has its own sequential data version
- Data version is stored in code and embedded in Feather metadata
- Sequential integers only (no dates)

Example:

    DATA_VERSIONS = {
        ("NSE", "EQUITY"): 5,
        ("NSE", "FO"): 3,
        ("NSE", "CURRENCY"): 2,
        ("BSE", "EQUITY"): 2,
        ("BSE", "FO"): 1,
    }

- Data versions increment only when that specific exchange–segment dataset changes

---

## 8. Stale Data Warnings

### 8.1 Warning Philosophy

- A light online check is performed to compare the local package version and data versions to the latest PyPI release
- Warnings indicate that the user is running an older package or data set
- Minimal online activity; calendar generation itself is offline

---

### 8.2 Behavior

- When a user accesses a calendar for a specific exchange–segment:
  1. Read local `data_version` from the Feather file
  2. Query the latest package release and latest data versions online (PyPI API or hosted JSON)
  3. Compare local vs latest
  4. Emit warnings.warn() only for that exchange–segment if outdated

Example warning:

    UserWarning: Your NSE–EQUITY calendar data (v5) is outdated.
    Upgrade marketnap to get the latest data.

- No warnings are emitted for unused exchanges or segments

---

## 9. Packaging & Build System

### 9.1 Build Backend

- setuptools
- Uses pyproject.toml (PEP 517 / PEP 621)
- No setup.py required

Reasons:
- Lightweight wheels
- Full control
- No tooling lock-in
- Easy embedding of package data

---

### 9.2 Package Data Inclusion

Feather files are included using:

    [tool.setuptools.package-data]
    "marketnap" = ["data/**/*.feather"]

---

### 9.3 Optional Dependencies

Optional dependencies may be defined using:

    [project.optional-dependencies]
    pandas = ["pandas"]

Users can install with:

    pip install marketnap[pandas]

---

## 10. Release & Development Workflow

---

### 10.1 Release Automation

- A pure Python script (release.py) is used
- Reasons:
  - Cross-platform
  - OS-independent
  - Fully customizable
- `release.py` now lives in `scripts/` alongside other future utility scripts

---

### 10.2 Responsibilities of release.py

- Bump __version__
- Increment DATA_VERSIONS per exchange–segment
- Ensure Feather metadata matches code
- Validate exchange–segment mappings
- Update CHANGELOG.md
- Build wheel and sdist
- Upload to PyPI

---

## 11. Project Structure (Proposed)

    marketnap/
    ├─ marketnap/
    │  ├─ __init__.py
    │  ├─ version.py
    │  ├─ calendar.py
    │  ├─ registry.py
    │  └─ data/
    │     ├─ nse/
    │     │  ├─ equity.feather
    │     │  ├─ fo.feather
    │     │  └─ currency.feather
    │     └─ bse/
    │        ├─ equity.feather
    │        └─ fo.feather
    ├─ scripts/
    │  └─ release.py
    ├─ docs/
    │  └─ PRD.md
    ├─ pyproject.toml
    ├─ README.md
    └─ CHANGELOG.md

---

## 12. Future Considerations

- Add more exchanges via new package releases
- Add new segments per exchange
- Optional Pandas or other backends
- More calendar utilities
- Optional special session timings (`start_time`, `end_time`)
- Better documentation and examples
- CI automation (optional)

---

## 13. Explicit Non-Goals (for now)

- No external data hosting for calendar data
- No runtime downloads for calendar data
- Calendar generation remains offline
- Background network activity limited to version checks only

---

## 14. Summary

MarketNap is designed to be:
- Simple
- Fast
- Offline
- Transparent
- Maintainer-controlled

Market data is sharded by exchange and market segment to enable:
- Efficient access
- Independent data versioning
- Precise stale data warnings

A minimal online check ensures the user knows if their package or calendar data is outdated.  
All data updates are deliberate and delivered via PyPI releases.
