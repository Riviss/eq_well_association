"""Example: rerun associations with generous spatial/temporal criteria."""

import pandas as pd
import eqassoc.config as cfg
import eqassoc.spatial as sp
from eqassoc.regions import assign_region
from eqassoc.process import process_batches
from sample_data import load_raw_sample_data


def main():
    # Load raw sample datasets (non-production)
    eq, hf_raw, wd_raw, prod_raw = load_raw_sample_data()

    # Broad search radii and long time windows
    cfg.update_params(
        radius_km={
            "HF": {"KSMMA": 20.0, "Northern Montney": 20.0},
            "WD": {"KSMMA": 20.0, "Northern Montney": 20.0},
            "PROD": {"KSMMA": 20.0, "Northern Montney": 20.0},
        },
        HF_Tmax_days=180,
        WD_Tmax_days=365,
        PROD_Tmax_days=365,
    )
    sp.PARAMS = cfg.PARAMS

    # Build time windows manually using updated parameters
    hf = hf_raw.copy()
    hf["inj_start_local"] = hf["datetime"]
    hf["decay_start_local"] = hf["datetime"]
    hf["inj_end_local"] = hf["inj_start_local"] + pd.Timedelta(days=cfg.PARAMS.HF_Tmax_days)
    hf = hf[[
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
    ]]
    hf = assign_region(hf)

    wd = wd_raw.copy()
    wd["inj_start_local"] = wd["yearmonth"].dt.to_period("M").dt.to_timestamp()
    wd["decay_start_local"] = wd["inj_start_local"] + pd.DateOffset(months=cfg.PARAMS.WD_delay_months)
    wd["inj_end_local"] = wd["inj_start_local"] + pd.Timedelta(days=cfg.PARAMS.WD_Tmax_days)
    wd = wd[[
        "well_id",
        "latitude",
        "longitude",
        "depth_km",
        "inj_start_local",
        "decay_start_local",
        "inj_end_local",
    ]]
    wd = assign_region(wd)

    prod = prod_raw.copy()
    prod["pad_id"] = prod["well_id"]
    prod["inj_start_local"] = prod["status_eff"]
    prod["decay_start_local"] = prod["status_eff"]
    prod["inj_end_local"] = prod["status_eff"] + pd.Timedelta(days=cfg.PARAMS.PROD_Tmax_days)
    prod = prod[[
        "well_id",
        "pad_id",
        "latitude",
        "longitude",
        "depth_km",
        "inj_start_local",
        "decay_start_local",
        "inj_end_local",
    ]]
    prod = assign_region(prod)

    assoc, cls = process_batches(
        eq_df=eq,
        hf=hf,
        wd=wd,
        prod=prod,
        lines_gdf=None,
        mode="detailed",
        batch=len(eq),
        in_memory=True,
        engine=None,
    )

    print("Generous association results")
    print(assoc)
    print()
    print(cls)


if __name__ == "__main__":
    main()
