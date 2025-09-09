"""
Example only: builds associations in-memory from bundled sample data and prints
the result. This DOES NOT write to your database.

To run a real, full rebuild that overwrites DB tables and excludes production,
use the root-level runner instead:
    python rerun_associations.py

Or call the CLI directly:
    PYTHONPATH=src python -m eqassoc.cli --mode full --types HF WD --verbose
"""

import pandas as pd
import eqassoc.config as cfg
import eqassoc.spatial as sp
from eqassoc.regions import assign_region
from eqassoc.process import process_batches
from sample_data import load_raw_sample_data


def main() -> None:
    print("[example] Running in-memory demo — no DB writes.")
    # Load raw sample datasets (non-production)
    eq, hf_raw, wd_raw, _ = load_raw_sample_data()

    # Use configuration parameters for spatial calculations
    sp.PARAMS = cfg.PARAMS

    # Build HF time windows using configured lags and window lengths
    hf = hf_raw.copy()
    hf["inj_start_local"] = (
        hf["datetime"]
        + pd.Timedelta(days=cfg.PARAMS.HF_lag_dateonly_days)
        + pd.Timedelta(hours=cfg.PARAMS.HF_lag_datetime_hours)
    )
    hf["decay_start_local"] = hf["inj_start_local"]
    hf["inj_end_local"] = hf["inj_start_local"] + pd.Timedelta(days=cfg.PARAMS.HF_Tmax_days)
    hf = hf[
        [
            "stage_id",
            "well_id",
            "pad_id",
            "formation",
            "latitude",
            "longitude",
            "depth_km",
            "inj_start_local",
            "decay_start_local",
            "inj_end_local",
        ]
    ]
    hf = assign_region(hf)

    # Build WD time windows using configured delays and window lengths
    wd = wd_raw.copy()
    wd["pad_id"] = wd["well_id"]
    wd["inj_start_local"] = wd["yearmonth"].dt.to_period("M").dt.to_timestamp()
    wd["decay_start_local"] = wd["inj_start_local"] + pd.DateOffset(months=cfg.PARAMS.WD_delay_months)
    wd["inj_end_local"] = wd["inj_start_local"] + pd.Timedelta(days=cfg.PARAMS.WD_Tmax_days)
    wd = wd[
        [
            "well_id",
            "pad_id",
            "latitude",
            "longitude",
            "depth_km",
            "inj_start_local",
            "decay_start_local",
            "inj_end_local",
        ]
    ]
    wd = assign_region(wd)

    # Rerun associations without production links
    assoc, cls = process_batches(
        eq_df=eq,
        hf=hf,
        wd=wd,
        prod=pd.DataFrame(),  # omit production
        lines_gdf=None,
        mode="detailed",
        batch=len(eq),
        in_memory=True,
        engine=None,
        types=["HF", "WD"],
    )

    print("Association results (HF + WD)")
    print(assoc)
    print()
    print(cls)


if __name__ == "__main__":
    main()
