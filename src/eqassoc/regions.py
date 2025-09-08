from __future__ import annotations
import numpy as np
import pandas as pd
from shapely.geometry import Point
from .config import PARAMS

def assign_region(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a 'region' column: 'KSMMA' if within KSMMA_poly, else 'Northern Montney'.
    Assumes df has numeric 'latitude' and 'longitude' in EPSG:4326 degrees.
    """
    poly = PARAMS.KSMMA_poly
    out = df.copy()
    out["region"] = np.where(
        out.apply(lambda r: poly.contains(Point(r.longitude, r.latitude)), axis=1),
        "KSMMA", "Northern Montney"
    )
    return out

