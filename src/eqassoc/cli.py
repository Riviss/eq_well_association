from __future__ import annotations
import argparse, logging, os
import pandas as pd
from sqlalchemy import inspect, text

from .config import DEFAULT_BATCH, DEFAULT_DB_URI, DEFAULT_EQ_TABLE, update_params
from .loaders import (
    load_engine,
    iter_earthquakes,
    load_target,
    load_hf_stage,
    load_hf_present_lines,
    load_wd,
    load_prod,
)
from .regions import assign_region
from .dbio import purge_obsolete_present, filter_incremental_eq
from .process import process_batches, backfill_missing_classified

log = logging.getLogger("eqassoc")

def _setup_logging(verbose: bool):
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s"
    )

def main():
    ap = argparse.ArgumentParser(
        description="Associate earthquakes with well activity",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument(
        "--assoc_mode",
        choices=["simple", "detailed"],
        default="detailed",
        help="Level of detail to store for each association",
    )
    ap.add_argument(
        "--n_jobs",
        type=int,
        default=1,
        help="Number of parallel jobs (reserved for compatibility; not used)",
    )
    ap.add_argument(
        "--mode",
        choices=["incremental", "full"],
        default="incremental",
        help="Processing mode: incremental appends new results, full rebuilds tables",
    )
    ap.add_argument(
        "--batch_size",
        type=int,
        default=DEFAULT_BATCH,
        help="Number of earthquakes to process per batch",
    )
    ap.add_argument(
        "--in_memory",
        action="store_true",
        help="Keep results in memory and write once after processing",
    )
    ap.add_argument("--verbose", action="store_true", help="Turn on DEBUG logging")
    ap.add_argument(
        "--reassociate_quake",
        type=str,
        help="Quake ID to force re-association for",
    )
    ap.add_argument(
        "--reassociate_wa",
        type=str,
        help="Well ID (wa_num) to force re-association for",
    )

    # Injection type and timing overrides
    ap.add_argument(
        "--types",
        nargs="+",
        choices=["HF", "WD", "PROD"],
        default=["HF", "WD", "PROD"],
        help="Activity types to include in association",
    )
    ap.add_argument(
        "--hf_lag_dateonly_days",
        type=int,
        help="Lag in days for HF events with date-only timestamps",
    )
    ap.add_argument(
        "--hf_lag_datetime_hours",
        type=int,
        help="Lag in hours for HF events with full timestamps",
    )
    ap.add_argument(
        "--hf_tmax_days",
        type=int,
        help="Maximum time window after HF activity in days",
    )
    ap.add_argument(
        "--wd_delay_months",
        type=int,
        help="Delay before WD associations become active, in months",
    )
    ap.add_argument(
        "--wd_tmax_days",
        type=int,
        help="Maximum time window after WD activity in days",
    )
    ap.add_argument(
        "--prod_tmax_days",
        type=int,
        help="Maximum time window after production activity in days",
    )
    ap.add_argument(
        "--time_decay_factor",
        type=float,
        help="Factor controlling probability decay; probability at Tmax is exp(-factor)",
    )
    args = ap.parse_args()

    _setup_logging(args.verbose)

    # Allow CLI to override time-window parameters
    update_params(
        HF_lag_dateonly_days=args.hf_lag_dateonly_days,
        HF_lag_datetime_hours=args.hf_lag_datetime_hours,
        HF_Tmax_days=args.hf_tmax_days,
        WD_delay_months=args.wd_delay_months,
        WD_Tmax_days=args.wd_tmax_days,
        PROD_Tmax_days=args.prod_tmax_days,
        time_decay_factor=args.time_decay_factor,
    )

    db_uri = os.getenv("EQ_DB_URI", DEFAULT_DB_URI)
    eng = load_engine(db_uri)

    log.info("Loading source tables …")
    eq_iter = iter_earthquakes(DEFAULT_EQ_TABLE, eng, args.batch_size)
    tgt = load_target()

    if "HF" in args.types:
        hf_stage = load_hf_stage(tgt)
        hf_present = load_hf_present_lines()
    else:
        hf_stage = pd.DataFrame()
        hf_present = None

    # if targeting a specific well for re-assoc: remove from stage so present applies
    if "HF" in args.types and args.reassociate_wa:
        wa = str(args.reassociate_wa)
        hf_stage = hf_stage[hf_stage["well_id"].astype(str) != wa]
        hf_present = hf_present[hf_present["well_id"].astype(str) == wa]
        log.debug("Re-association filter: removed WA %s from stage, kept in present", wa)

    stage_wells = set(hf_stage["well_id"].unique()) if "HF" in args.types else set()
    # filter present by stage-covered wells (present is only for missing stage)
    if "HF" in args.types:
        hf_present = hf_present[~hf_present["well_id"].isin(stage_wells)]

    affected = purge_obsolete_present(stage_wells, eng) if "HF" in args.types else set()

    wd = load_wd() if "WD" in args.types else pd.DataFrame()
    prod = load_prod() if "PROD" in args.types else pd.DataFrame()

    # debug counts
    if "HF" in args.types:
        try:
            # cheap re-load for logging parity with original
            present_all = load_hf_present_lines()
            log.debug(
                "Counts → stage: %d, present(before): %d, present(kept): %d",
                len(hf_stage), len(present_all), len(hf_present)
            )
        except Exception:
            pass

    if args.mode == "full" and not args.in_memory:
        # SQLAlchemy 2.0: use explicit connection/transaction for DDL
        with eng.begin() as con:
            for t in ("eq_well_association", "eq_well_association_classified"):
                if inspect(eng).has_table(t):
                    con.exec_driver_sql(f"TRUNCATE TABLE {t}")
    qid = str(args.reassociate_quake) if args.reassociate_quake else None
    processed_any = False
    inmem_assoc, inmem_cls = [], []
    for eq in eq_iter:
        eq = assign_region(eq)
        if qid:
            eq = eq[eq["quake_id"] == qid]
            if eq.empty:
                continue
            log.debug(
                "Filtered eq to only quake_id %s (remaining %d rows)",
                qid,
                len(eq),
            )
        if args.mode == "incremental":
            eq = filter_incremental_eq(eq, eng, affected)
            if eq.empty:
                continue
        assoc, cls = process_batches(
            eq_df=eq,
            hf=hf_stage,
            wd=wd,
            prod=prod,
            lines_gdf=hf_present,
            mode=args.assoc_mode,
            batch=args.batch_size,
            in_memory=args.in_memory,
            engine=eng,
            target_quake=args.reassociate_quake,
            target_wa=args.reassociate_wa,
            types=args.types,
        )
        processed_any = True
        if args.in_memory and assoc is not None:
            inmem_assoc.append(assoc)
            inmem_cls.append(cls)

    if not processed_any:
        log.info("Nothing to process — exit.")
        return

    backfill_missing_classified(eng)

    if args.in_memory and inmem_assoc:
        assoc = pd.concat(inmem_assoc, ignore_index=True)
        cls = pd.concat(inmem_cls, ignore_index=True)
        assoc.to_sql(
            "eq_well_association",
            eng,
            if_exists=("replace" if args.mode == "full" else "append"),
            index=False,
        )
        cls.to_sql(
            "eq_well_association_classified",
            eng,
            if_exists=("replace" if args.mode == "full" else "append"),
            index=False,
        )

    log.info("Done.")

if __name__ == "__main__":
    main()
