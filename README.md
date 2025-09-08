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

Earthquakes are streamed from the database in batches (default 10k) so full
re-runs do not exhaust memory.  The batch size can be adjusted with
`--batch_size` if needed.

The earthquake source table defaults to `master_origin_3D`. Use `--eq_table`
to select from `master_origin`, `master_origin_3D`, or `hybrid_catalog`.

Env:

EQ_DB_URI (default: mysql+pymysql://root@localhost/earthquakes)

data path in code defaults to /home/pgcseiscomp/Documents/bcer_data
