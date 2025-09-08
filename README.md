# eqassoc

Resolution-aware earthquake–well association (stage vs present), CRS-safe, with incremental/full modes.

## Install (editable)
```bash
pip install -e .

# full refresh
eq-assoc --mode full --assoc_mode detailed

## Usage

# incremental update
eq-assoc --mode incremental --assoc_mode detailed

# verbose, target quake and well
eq-assoc --mode incremental --assoc_mode detailed --verbose \
  --reassociate_quake 12345 --reassociate_wa 67890

Env:

EQ_DB_URI (default: mysql+pymysql://root@localhost/earthquakes)

data path in code defaults to /home/pgcseiscomp/Documents/bcer_data
