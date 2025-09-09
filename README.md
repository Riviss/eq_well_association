# eqassoc

Resolution-aware earthquake–well association (stage vs present) that can refresh in full or update incrementally while remaining CRS-safe.

## Installation

Install the package in editable mode:

```bash
pip install -e .
```

## Command-line usage

The project exposes a console script `eq-assoc`. Common invocations include:

### Full database refresh

Rebuild all associations from scratch, truncating existing result tables.

```bash
eq-assoc --mode full --assoc_mode detailed
```

### Incremental update

Process only earthquakes that have not been associated yet and append the results.

```bash
eq-assoc --mode incremental --assoc_mode detailed
```

### Verbose run targeting specific quake and well

Output debug logs and force re-association for a single quake and well.

```bash
eq-assoc --mode incremental --assoc_mode detailed --verbose \
  --reassociate_quake 12345 --reassociate_wa 67890
```

### Batch size and in-memory mode

Earthquakes are streamed from the database in batches (default 10k) so full re-runs do not exhaust memory. Adjust the size and keep results in memory before writing:

```bash
eq-assoc --mode incremental --batch_size 5000 --in_memory
```

### Controlling activity types and time windows

By default the association considers hydraulic fracturing (HF), water disposal
(WD) and production (PROD) activities.  Use `--types` to specify a subset of
these, e.g. to omit production wells:

```bash
eq-assoc --mode incremental --types HF WD
```

Time-window parameters for each activity type can also be overridden from the
CLI. For example, to shorten the HF association tail:

```bash
eq-assoc --hf_tmax_days 365
```

## Environment

The CLI reads the database connection string from the `EQ_DB_URI` environment variable; the default is `mysql+pymysql://root@localhost/earthquakes`.

Data files are expected under `/home/pgcseiscomp/Documents/bcer_data` unless configured otherwise.

