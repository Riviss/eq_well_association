from __future__ import annotations
import math
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from shapely.ops import nearest_points
from sklearn.neighbors import BallTree
from .config import PARAMS, PLANE_EPSG
from .time_windows import exp_decay, gaussian_distance

def haversine_tree(latlon_deg: np.ndarray) -> BallTree:
    """BallTree in radians for great-circle queries."""
    return BallTree(np.radians(latlon_deg), metric="haversine")

def assoc_points_batch(
    eq: pd.DataFrame,
    src: pd.DataFrame,
    tree: BallTree,
    typ: str,
    mode: str,
    target_quake: int | None = None,
    target_wa: str | None = None,
) -> pd.DataFrame:
    """
    Associate earthquakes with point-based activities (HF, WD, PROD).
    Returns: DataFrame with
    ['quake_id','stage_id','well_id','pad_id','type','d_km','dt_days',
     'score','region','resolution']
    """
    R_e = PARAMS.earth_R_km
    parts: list[pd.DataFrame] = []

    # region-wise query to honor different radii
    for region, R_km in PARAMS.radius_km[typ].items():
        sub = eq[eq["region"] == region]
        if sub.empty:
            continue
        idxs, dists = tree.query_radius(
            np.radians(sub[["latitude", "longitude"]]),
            r=R_km / R_e,
            return_distance=True,
        )
        if not any(len(a) for a in idxs):
            continue

        tmp = pd.DataFrame(
            {
                "quake_idx": np.repeat(sub.index.values, [len(a) for a in idxs]).astype("int32"),
                "src_idx": np.concatenate(idxs).astype("int32"),
                "d_km": (np.concatenate(dists) * R_e).astype("float32"),
            }
        ).merge(
            sub[["quake_id", "time_local", "region"]],
            left_on="quake_idx",
            right_index=True,
        )
        tmp["type"] = typ
        parts.append(tmp)

    if not parts:
        return pd.DataFrame()

    neigh = pd.concat(parts, ignore_index=True)

    # merge src metadata; avoid duplicate 'region'
    src_meta = (src.reset_index()
                  .rename(columns={"index": "src_idx"})
                  .drop(columns=["region"], errors="ignore"))
    neigh = neigh.merge(src_meta, on="src_idx", how="left", validate="m:1")

    if "resolution" not in neigh.columns:
        neigh["resolution"] = "stage"

    # time window gates and dt
    if typ == "PROD":
        valid = neigh["time_local"] >= neigh["inj_start_local"]
        dt = (neigh["time_local"] - neigh["inj_start_local"]).dt.total_seconds() / 86400.0
    else:
        valid = (neigh["time_local"] >= neigh["inj_start_local"]) & (neigh["time_local"] <= neigh["inj_end_local"])
        dt = (neigh["time_local"] - neigh["decay_start_local"]).dt.total_seconds() / 86400.0

    neigh = neigh[valid].copy()
    neigh["dt_days"] = np.clip(dt.loc[neigh.index].astype("float32"), 0.0, None)

    # kernels
    if mode == "detailed":
        sigma = neigh["region"].map(lambda r: PARAMS.radius_km[typ][r] / 2.45).astype("float32")
        neigh["f_d"] = gaussian_distance(neigh["d_km"].values, sigma.values)
        tau = PARAMS.HF_Tmax_days / 2.45 if typ == "HF" else (PARAMS.WD_Tmax_days / 2.45 if typ == "WD" else PARAMS.PROD_Tmax_days / 2.45)
        neigh["f_t"] = exp_decay(neigh["dt_days"].values, tau)
    else:
        neigh["f_d"] = 1.0
        neigh["f_t"] = 1.0

    # scoring
    w_type = PARAMS.weights[typ]
    if typ == "HF":
        w_form = neigh["formation"].map(PARAMS.weights["formation"]).fillna(0.2).astype("float32")
        neigh["score"] = (w_type * w_form * neigh["f_d"] * neigh["f_t"]).astype("float32")
    else:
        neigh["score"] = (w_type * neigh["f_d"] * neigh["f_t"]).astype("float32")

    neigh["stage_id"] = neigh.get("stage_id")
    neigh["pad_id"] = neigh.get("pad_id")

    # focused debug (no-op on output)
    if target_quake and target_wa:
        _ = neigh[(neigh["quake_id"] == int(target_quake)) & (neigh["well_id"].astype(str) == str(target_wa))]

    return neigh[["quake_id","stage_id","well_id","pad_id","type","d_km","dt_days","score","region","resolution"]]

def assoc_lines_batch(
    eq: pd.DataFrame,
    lines: gpd.GeoDataFrame,
    mode: str,
    target_quake: int | None = None,
    target_wa: str | None = None
) -> pd.DataFrame:
    """Associate earthquakes (points) with HF present lines in EPSG:26910."""
    if lines.empty or eq.empty:
        return pd.DataFrame()
    eqg = gpd.GeoDataFrame(
        eq,
        geometry=gpd.points_from_xy(eq["longitude"], eq["latitude"]),
        crs="EPSG:4326"
    ).to_crs(epsg=PLANE_EPSG)

    R_map = PARAMS.radius_km["HF"]
    tau = PARAMS.HF_Tmax_days / 2.45
    rec = []

    # Iterate quakes with region-based buffer
    for _, q in eqg.iterrows():
        buf_m = R_map[q.region] * 1000.0
        buf = q.geometry.buffer(buf_m)
        for idx in lines.sindex.intersection(buf.bounds):
            ln = lines.iloc[idx]
            if not ln.prep.intersects(buf):
                continue
            # compute shortest distance from the earthquake to the well line
            pt_eq, pt_ln = nearest_points(q.geometry, ln.geometry)
            d_m = pt_eq.distance(pt_ln)
            if d_m > buf_m:
                continue
            t = q.time_local
            if not (ln.inj_start_local <= t <= ln.inj_end_local):
                continue
            dt = max(0.0, (t - ln.decay_start_local).total_seconds() / 86400.0)
            if mode == "simple":
                f_d, f_t = 1.0, 1.0
            else:
                sigma_km = R_map[q.region] / 2.45
                f_d = math.exp(-((d_m/1000.0)**2) / (2.0 * (sigma_km**2)))
                f_t = float(exp_decay(np.array([dt], dtype="float32"), tau)[0])
            score = PARAMS.weights["HF"] * PARAMS.weights["formation"]["Other"] * f_d * f_t
            rec.append({
                "quake_id": int(q.quake_id), "stage_id": None,
                "well_id": ln.well_id, "pad_id": ln.well_id,
                "type":"HF","d_km":np.float32(d_m/1000.0),
                "dt_days":np.float32(dt),"score":np.float32(score),
                "region":q.region, "resolution":"present"
            })
    df = pd.DataFrame.from_records(rec)
    if df.empty:
        return df

    # focused debug (no-op on output)
    if target_quake and target_wa:
        _ = df[(df["quake_id"] == int(target_quake)) & (df["well_id"].astype(str) == str(target_wa))]

    return df

