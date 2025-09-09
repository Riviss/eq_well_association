from __future__ import annotations
import logging
import numpy as np
import pandas as pd
from sqlalchemy import inspect, text

from .spatial import haversine_tree, assoc_points_batch, assoc_lines_batch

log = logging.getLogger(__name__)

EXP_COLS = ["quake_id","stage_id","well_id","pad_id",
            "type","d_km","dt_days","score","region","resolution"]

def batch_iter(df: pd.DataFrame, size: int):
    for start in range(0, len(df), size):
        yield df.iloc[start:start+size].copy()

def process_batches(
    eq_df: pd.DataFrame,
    hf: pd.DataFrame,
    wd: pd.DataFrame,
    prod: pd.DataFrame,
    lines_gdf,
    mode: str,
    batch: int,
    in_memory: bool,
    engine,
    target_quake: int | None = None,
    target_wa: str | None = None,
    types: list[str] | None = None,
):
    types = types or ["HF", "WD", "PROD"]
    trees = {}
    srcs = {}
    if "HF" in types and not hf.empty:
        trees["HF"] = haversine_tree(hf[["latitude", "longitude"]])
        srcs["HF"] = hf
    if "WD" in types and not wd.empty:
        trees["WD"] = haversine_tree(wd[["latitude", "longitude"]])
        srcs["WD"] = wd
    if "PROD" in types and not prod.empty:
        trees["PROD"] = haversine_tree(prod[["latitude", "longitude"]])
        srcs["PROD"] = prod

    all_assoc, all_cls = [], []
    for b, eq_batch in enumerate(batch_iter(eq_df, batch), 1):
        log.info("Batch %d (%d quakes)…", b, len(eq_batch))
        parts = []
        for t in types:
            if t not in srcs:
                continue
            parts.append(
                assoc_points_batch(
                    eq_batch, srcs[t], trees[t], t, mode, target_quake, target_wa
                )
            )
        if "HF" in types and lines_gdf is not None:
            parts.append(
                assoc_lines_batch(
                    eq_batch, lines_gdf, mode, target_quake, target_wa
                )
            )

        # normalize columns/order
        for i, df in enumerate(parts):
            parts[i] = (df.loc[:, ~df.columns.duplicated()]
                          .reindex(columns=EXP_COLS, fill_value=pd.NA))
        assoc = pd.concat(parts, ignore_index=True, sort=False)
        if assoc.empty:
            continue

        counts = assoc["type"].value_counts().reindex(
            ["HF", "WD", "PROD"], fill_value=0
        )
        log.debug(
            "Batch %d counts: HF=%d, WD=%d, PROD=%d, total=%d",
            b,
            counts.get("HF", 0),
            counts.get("WD", 0),
            counts.get("PROD", 0),
            len(assoc),
        )

        # per-stage probability among all links for a quake
        assoc["P_stage"] = assoc.groupby("quake_id")["score"].transform(lambda x: (x / x.sum()).astype("float32"))

        # best stage (for d_km/dt_days too)
        best_stage = assoc.loc[assoc.groupby("quake_id")["P_stage"].idxmax(),
                               ["quake_id","stage_id","P_stage"]] \
                          .rename(columns={"stage_id":"best_stage", "P_stage":"best_stage_prob"})
        best_dt = assoc.loc[assoc.groupby("quake_id")["P_stage"].idxmax(),
                            ["quake_id","d_km","dt_days"]] \
                       .rename(columns={"d_km":"best_d_km","dt_days":"best_dt_days"})

        # well-level collapse
        well = assoc.groupby(["quake_id","well_id","type"])["P_stage"].sum().rename("P_well").reset_index()
        well["P_well"] = well.groupby("quake_id")["P_well"].transform(lambda x: (x/x.sum()).astype("float32"))
        best_well = well.loc[well.groupby("quake_id")["P_well"].idxmax()] \
                        .rename(columns={"well_id":"best_well","type":"best_well_type","P_well":"best_well_prob"})[
                        ["quake_id","best_well","best_well_type","best_well_prob"]]

        # pad-level collapse
        pad = assoc.groupby(["quake_id","pad_id"])["P_stage"].sum().rename("P_pad").reset_index()
        pad["P_pad"] = pad.groupby("quake_id")["P_pad"].transform(lambda x: (x/x.sum()).astype("float32"))
        best_pad = pad.loc[pad.groupby("quake_id")["P_pad"].idxmax()] \
                       .rename(columns={"pad_id":"best_pad","P_pad":"best_pad_prob"})[
                       ["quake_id","best_pad","best_pad_prob"]]

        # counts
        cts = (
            assoc.groupby(["quake_id", "type"])["well_id"]
            .nunique()
            .unstack(fill_value=0)
            .reindex(["HF", "WD", "PROD"], axis=1, fill_value=0)
            .astype(int)
        )
        cts.columns = [f"n_{c.lower()}_wells" for c in cts.columns]
        cts = cts.reset_index()

        cls = best_stage.merge(best_well, on="quake_id") \
                        .merge(best_pad, on="quake_id") \
                        .merge(best_dt,  on="quake_id") \
                        .merge(cts,      on="quake_id")

        if in_memory:
            all_assoc.append(assoc); all_cls.append(cls)
        else:
            assoc.to_sql("eq_well_association", engine, if_exists="append", index=False)
            cls.to_sql("eq_well_association_classified", engine, if_exists="append", index=False)

    if in_memory and all_assoc:
        return (pd.concat(all_assoc, ignore_index=True),
                pd.concat(all_cls,   ignore_index=True))
    return None, None

def backfill_missing_classified(engine, batch=10_000, logger: logging.Logger | None = None):
    """
    Idempotent back-fill: rebuild classified rows missing for quakes in association.
    Mirrors original aggregation logic.
    """
    logg = logger or log
    EXP_CLS_COLS = [
        "quake_id",
        "best_stage", "best_stage_prob",
        "best_well", "best_well_type", "best_well_prob",
        "best_pad", "best_pad_prob",
        "best_d_km", "best_dt_days",
        "n_hf_wells", "n_prod_wells", "n_wd_wells",
        "best_well_target", "best_well_formation",
    ]
    with engine.begin() as con:
        missing_qids = con.execute(text("""
            SELECT DISTINCT e.quake_id
            FROM   eq_well_association e
            LEFT   JOIN eq_well_association_classified c
                   ON e.quake_id = c.quake_id
            WHERE  c.quake_id IS NULL
        """)).scalars().all()
    if not missing_qids:
        logg.info("No missing classified rows – nothing to back-fill.")
        return

    logg.info("Back-filling %d quake(s) into eq_well_association_classified …", len(missing_qids))

    done_cls = []
    for i in range(0, len(missing_qids), batch):
        q_chunk = missing_qids[i:i+batch]
        ph = ",".join(["%s"] * len(q_chunk))
        assoc = pd.read_sql_query(f"""
            SELECT quake_id, stage_id, well_id, pad_id, type,
                   d_km, dt_days, score, P_stage
            FROM   eq_well_association
            WHERE  quake_id IN ({ph})
        """, engine, params=q_chunk)

        if assoc.empty:
            continue

        assoc["P_stage"] = assoc["P_stage"].fillna(0.0).astype("float32")

        idx = assoc.groupby("quake_id")["P_stage"].idxmax()
        best_stage = (assoc.loc[idx, ["quake_id","stage_id","P_stage"]]
                          .rename(columns={"stage_id":"best_stage","P_stage":"best_stage_prob"}))

        well = assoc.groupby(["quake_id","well_id","type"])["P_stage"].sum().rename("P_well").reset_index()
        well["P_well"] = well.groupby("quake_id")["P_well"].transform(lambda x: (x/x.sum()).astype("float32"))
        idx = well.groupby("quake_id")["P_well"].idxmax()
        best_well = (well.loc[idx]
                          .rename(columns={"well_id":"best_well","type":"best_well_type","P_well":"best_well_prob"}))

        pad = assoc.groupby(["quake_id","pad_id"])["P_stage"].sum().rename("P_pad").reset_index()
        pad["P_pad"] = pad.groupby("quake_id")["P_pad"].transform(lambda x: (x/x.sum()).astype("float32"))
        idx = pad.groupby("quake_id")["P_pad"].idxmax()
        best_pad = (pad.loc[idx]
                        .rename(columns={"pad_id":"best_pad","P_pad":"best_pad_prob"}))

        best_dt = assoc.loc[assoc.groupby("quake_id")["P_stage"].idxmax(), ["quake_id","d_km","dt_days"]] \
                       .rename(columns={"d_km":"best_d_km","dt_days":"best_dt_days"})

        cts = assoc.groupby(["quake_id","type"])["well_id"].nunique().unstack(fill_value=0).astype(int)
        cts.columns = [f"n_{c.lower()}_wells" for c in cts.columns]
        cts = cts.reset_index()

        cls = (best_stage.merge(best_well, on="quake_id", how="left")
                        .merge(best_pad,  on="quake_id", how="left")
                        .merge(best_dt,   on="quake_id", how="left")
                        .merge(cts,       on="quake_id", how="left"))
        for col in ("n_hf_wells","n_prod_wells","n_wd_wells"):
            if col not in cls:
                cls[col] = 0
        cls["best_well_target"]    = pd.NA
        cls["best_well_formation"] = pd.NA
        cls = cls.reindex(columns=EXP_CLS_COLS)
        done_cls.append(cls)

    if done_cls:
        out = pd.concat(done_cls, ignore_index=True)
        out.to_sql("eq_well_association_classified", engine, if_exists="append", index=False)
        logg.info("Inserted %d new classified row(s).", len(out))

