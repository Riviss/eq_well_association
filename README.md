# eqassoc

Resolution-aware earthquake–well association (stage vs present), CRS-safe, with incremental or full refresh modes.

## Installation

```bash
pip install -e .
```

## Command-line usage

The package installs an `eq-assoc` command for running the association pipeline.

### Incremental update (default)

```bash
eq-assoc --mode incremental --assoc_mode detailed
```

### Full refresh of association tables

```bash
eq-assoc --mode full --assoc_mode detailed
```

### Verbose run targeting a specific quake and well

```bash
eq-assoc --mode incremental --assoc_mode detailed --verbose \
  --reassociate_quake 12345 --reassociate_wa 67890
```

### Other useful options

- `--batch_size N` – set earthquake batch size (default 10,000)
- `--in_memory` – process in memory and write results when finished
- `--reassociate_quake ID` – force re-association for a particular quake
- `--reassociate_wa ID` – force re-association for a particular well

Earthquakes are streamed from the database in batches so full re-runs do not exhaust memory.

## Environment

- `EQ_DB_URI` – database URI (default `mysql+pymysql://root@localhost/earthquakes`)

The default data path in code is `/home/pgcseiscomp/Documents/bcer_data`.
