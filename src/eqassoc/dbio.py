from __future__ import annotations
from typing import Set
import pandas as pd
from sqlalchemy import text, inspect

def purge_obsolete_present(stage_wells: Set[str], engine) -> Set[int]:
    """
    If a well has stage data, remove any 'present' resolution rows for that well.
    Returns the set of affected quake_ids for reprocessing (same as original).
    """
    if not stage_wells:
        return set()
    wlist = ",".join(f"'{w}'" for w in stage_wells)
    with engine.begin() as con:
        qids = con.execute(text(f"""
            SELECT DISTINCT quake_id FROM eq_well_association
            WHERE resolution='present' AND well_id IN ({wlist})
        """)).scalars().all()
        con.execute(text(f"""
            DELETE FROM eq_well_association
            WHERE resolution='present' AND well_id IN ({wlist})
        """))
        if qids:
            qlist = ",".join(str(q) for q in qids)
            con.execute(text(f"""
                DELETE FROM eq_well_association_classified
                WHERE quake_id IN ({qlist})
            """))
    return set(qids)

def filter_incremental_eq(eq: pd.DataFrame, engine, affected_qids: Set[int]) -> pd.DataFrame:
    """
    Incremental logic parity with original:
    - If affected wells exist: process only those quake_ids.
    - Else if eq_well_association exists: skip already-done quakes.
    """
    if affected_qids:
        return eq[eq["quake_id"].isin(affected_qids)]
    insp = inspect(engine)
    if insp.has_table("eq_well_association"):
        done = set(pd.read_sql_table("eq_well_association", engine)["quake_id"])
        return eq[~eq["quake_id"].isin(done)]
    return eq

