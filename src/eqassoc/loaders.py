from __future__ import annotations
from pathlib import Path
import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString
from shapely.prepared import prep

from sqlalchemy import create_engine
from .config import PARAMS, PLANE_EPSG, PACIFIC_TZ
from .regions import assign_region
from .time_windows import (
    localize_to_pacific,
    hf_stage_window,
    hf_present_line_window,
    wd_window,
    prod_window,
)

DATAPATH = Path("/home/pgcseiscomp/Documents/bcer_data")

def load_engine(db_uri: str):
    return create_engine(db_uri)

def load_earthquakes(tbl: str, eng):
    df = pd.read_sql_table(tbl, eng)[["master_id", "lat", "lon", "depth", "datetime"]]
    df = df.rename(columns={"master_id": "quake_id", "lat": "latitude",
                            "lon": "longitude", "depth": "depth_km"})
    df["time_local"] = localize_to_pacific(df["datetime"])
    df = df.dropna(subset=["latitude", "longitude", "time_local"]).reset_index(drop=True)
    return df[["quake_id", "latitude", "longitude", "depth_km", "time_local"]]

def load_target():
    t = pd.read_csv(DATAPATH/"from_corrie"/"target.csv", dtype={"WA": str})
    t["formation"] = (t["HZ_well_Target Value"] == "LM").map({True:"Lower Middle Montney", False:"Other"})
    return t[["WA", "formation"]].rename(columns={"WA": "well_id"})

def load_hf_stage(target: pd.DataFrame):
    hf = pd.read_csv(DATAPATH/"extracted"/"hf_latlon.csv",
                     parse_dates=["datetime", "date"],
                     dtype={"wa_num": str, "pad_operation_ID": str})
    hf = hf.rename(columns={"FRAC STAGE NUM": "stage_id",
                            "wa_num": "well_id",
                            "pad_operation_ID": "pad_id",
                            "lat": "latitude",
                            "lon": "longitude",
                            "TVDss": "depth_m"})
    hf["depth_km"] = hf["depth_m"] / 1000.0
    hf = hf.merge(target, on="well_id", how="left").fillna({"formation": "Other"})
    hf = hf_stage_window(hf)
    hf["resolution"] = "stage"
    hf = hf[["stage_id", "well_id", "pad_id", "formation",
             "latitude", "longitude", "depth_km",
             "inj_start_local", "decay_start_local", "inj_end_local",
             "resolution"]]
    return assign_region(hf)

def load_hf_present_lines():
    df = pd.read_csv(
        DATAPATH / "extracted" / "hf_latlon_present.csv",
        parse_dates=["Expected Start Date", "Expected End Date"],
        low_memory=False,
    )
    df = df[df["wa_num"].notnull()].copy()
    df["wa_num"] = df["wa_num"].astype(int).astype(str)

    rows = []
    for well, g in df.groupby("wa_num"):
        coords = list(zip(g["lon"].values, g["lat"].values))
        if len(coords) == 1:
            coords = coords * 2
        line = LineString(coords)

        w = hf_present_line_window(g)
        rows.append(
            {
                "well_id": well,
                "geometry": line,
                **w
            }
        )

    gdf = gpd.GeoDataFrame(rows, crs="EPSG:4326").to_crs(epsg=PLANE_EPSG)
    gdf["prep"] = gdf.geometry.apply(prep)
    gdf["formation"] = "Other"
    # NOTE: These 'latitude/longitude' are centroid coords in EPSG:26910 (meters).
    # They are not used for region decisions in line association; keep parity with original.
    gdf["latitude"] = gdf.geometry.centroid.y
    gdf["longitude"] = gdf.geometry.centroid.x
    gdf["resolution"] = "present"
    # Assign region not used downstream for lines but keep parity
    gdf = assign_region(gdf)
    return gdf

def load_wd():
    wd = pd.read_csv(DATAPATH/"extracted"/"wd_latlon.csv",
                     parse_dates=["yearmonth"], dtype={"wa_num": str})
    wd = wd.rename(columns={"wa_num": "well_id",
                            "lat": "latitude",
                            "lon": "longitude",
                            "TVDss": "depth_m"})
    wd["depth_km"] = wd["depth_m"] / 1000.0
    wd = wd_window(wd)
    wd = wd[["well_id", "latitude", "longitude", "depth_km",
             "inj_start_local", "decay_start_local", "inj_end_local"]]
    return assign_region(wd)

def load_prod():
    cut = PARAMS.prod_cut
    ws = pd.read_csv(DATAPATH/"extracted"/"w_status.csv",
                     skiprows=1, dtype={"Wa Num": str})
    ws["status_eff"] = pd.to_datetime(ws["Status Eff Date"], format="%Y%m%d",
                                      errors="coerce")
    ws = ws[(ws["Mode Code"] == "ACT") &
            (ws["Ops Type"] == "PROD") &
            ws["status_eff"].notna() & (ws["status_eff"] >= cut)]
    wells = pd.read_csv(DATAPATH/"extracted"/"wells_latlon.csv",
                        dtype={"wa_num": str}).rename(
        columns={"wa_num": "well_id", "lat": "latitude", "lon": "longitude"})
    prod = (ws[["Wa Num", "status_eff"]]
            .rename(columns={"Wa Num": "well_id"})
            .merge(wells, on="well_id"))
    prod["depth_km"] = 2.0
    prod = prod_window(prod)
    prod = prod[["well_id", "latitude", "longitude", "depth_km",
                 "inj_start_local", "decay_start_local", "inj_end_local"]]
    return assign_region(prod)

