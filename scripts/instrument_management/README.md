# Instrument Management Scripts

This directory contains scripts for managing financial instruments in the ATS system.

## Scripts

- **add_instruments.py**: Adds new instruments to the database in Kubernetes environments.
- **check_instrument_count.py**: Verifies the count of instruments in the database.
- **check_instrument_xrefs.py**: Checks cross-references between instruments and vendor symbols.
- **verify_instrument_data.py**: Validates instrument data for consistency and completeness.

## Usage

These scripts can be run directly:

```bash
python scripts/instrument_management/add_instruments.py
python scripts/instrument_management/check_instrument_count.py
```

Most scripts interact with the database in Kubernetes and may require appropriate kubectl configuration and permissions.
