from __future__ import annotations
import argparse, logging, os
import pandas as pd
from sqlalchemy import inspect, text

from .config import DEFAULT_BATCH, DEFAULT_DB_URI
from .loaders import load_engine, load_earthquakes, load_target, load_hf_stage, load_hf_present_lines, load_wd, load_prod
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
    ap = argparse.ArgumentParser()
    ap.add_argument("--assoc_mode", choices=["simple","detailed"], default="detailed")
    ap.add_argument("--n_jobs", type=int, default=1)  # kept for parity, not used
    ap.add_argument("--mode", choices=["incremental","full"], default="incremental")
    ap.add_argument("--batch_size", type=int, default=DEFAULT_BATCH)
    ap.add_argument("--in_memory", action="store_true")
    ap.add_argument("--verbose", action="store_true", help="turn on DEBUG logging")
    ap.add_argument("--reassociate_quake", type=str, help="quake_id to force re-association for")
    ap.add_argument("--reassociate_wa", type=str, help="well_id (wa_num) to force re-association for")
    args = ap.parse_args()

    _setup_logging(args.verbose)

    db_uri = os.getenv("EQ_DB_URI", DEFAULT_DB_URI)
    eng = load_engine(db_uri)

    log.info("Loading source tables …")
    eq = assign_region(load_earthquakes("master_origin_3D", eng))
    tgt = load_target()
    hf_stage = load_hf_stage(tgt)
    hf_present = load_hf_present_lines()

    # if targeting a specific well for re-assoc: remove from stage so present applies
    if args.reassociate_wa:
        wa = str(args.reassociate_wa)
        hf_stage = hf_stage[hf_stage["well_id"].astype(str) != wa]
        hf_present = hf_present[hf_present["well_id"].astype(str) == wa]
        log.debug("Re-association filter: removed WA %s from stage, kept in present", wa)

    stage_wells = set(hf_stage["well_id"].unique())
    # filter present by stage-covered wells (present is only for missing stage)
    hf_present = hf_present[~hf_present["well_id"].isin(stage_wells)]

    affected = purge_obsolete_present(stage_wells, eng)

    wd = load_wd()
    prod = load_prod()

    # debug counts
    try:
        # cheap re-load for logging parity with original
        present_all = load_hf_present_lines()
        log.debug("Counts → stage: %d, present(before): %d, present(kept): %d",
                  len(hf_stage), len(present_all), len(hf_present))
    except Exception:
        pass

    # optional quake targeting
    if args.reassociate_quake:
        qid = str(args.reassociate_quake)
        eq = eq[eq["quake_id"] == qid]
        log.debug("Filtered eq to only quake_id %s (remaining %d rows)", qid, len(eq))

    if args.mode == "incremental":
        eq = filter_incremental_eq(eq, eng, affected)
        if eq.empty:
            log.info("Nothing to process — exit.")
            return

    if args.mode == "full" and not args.in_memory:
        for t in ("eq_well_association", "eq_well_association_classified"):
            if inspect(eng).has_table(t):
                eng.execute(text(f"TRUNCATE TABLE {t}"))

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
        target_wa=args.reassociate_wa
    )

    backfill_missing_classified(eng)

    if args.in_memory and assoc is not None:
        assoc.to_sql("eq_well_association", eng,
                     if_exists=("replace" if args.mode=="full" else "append"),
                     index=False)
        cls.to_sql("eq_well_association_classified", eng,
                   if_exists=("replace" if args.mode=="full" else "append"),
                   index=False)

    log.info("Done.")

if __name__ == "__main__":
    main()

