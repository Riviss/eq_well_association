from __future__ import annotations
import math
import numpy as np
import pandas as pd
from pandas import DateOffset
from .config import PARAMS, FORT_ST_JOHN_TZ

def utc_to_fort_st_john(ts: pd.Series) -> pd.Series:
    """Convert UTC timestamps to Fort St. John local time."""
    return (
        pd.to_datetime(ts, utc=True)
          .dt.tz_convert(FORT_ST_JOHN_TZ)
          .dt.tz_localize(None)
    )

def exp_decay(dt_days: np.ndarray, tau_days: float) -> np.ndarray:
    """Vectorized exp decay; dt_days >= 0. Uses e^(-dt/tau)."""
    arr = np.asarray(dt_days, dtype="float32")
    return np.where(arr == 0.0, 1.0, np.exp(-arr / tau_days)).astype("float32")

def gaussian_distance(d_km: np.ndarray, sigma_km: np.ndarray) -> np.ndarray:
    """exp(-(d^2)/(2 sigma^2)) with vectorized broadcasting."""
    d = np.asarray(d_km, dtype="float32")
    s = np.asarray(sigma_km, dtype="float32")
    return np.exp(-(d**2) / (2.0 * (s**2))).astype("float32")

# --- Time-window builders (explicit & robust) -----------------------------

def hf_stage_window(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build window for HF stages when we have precise datetime or just a date.
    - If 'datetime' present: lag by HF_lag_datetime_hours.
    - Else: use 'date' and lag by HF_lag_dateonly_days.
    inj_end = inj_start + HF_Tmax_days.
    """
    p = PARAMS
    out = df.copy()
    out["inj_base"] = out["datetime"].where(out["datetime"].notna(), out["date"])
    out["decay_start_local"] = np.where(
        out["datetime"].notna(),
        out["inj_base"] + DateOffset(hours=p.HF_lag_datetime_hours),
        out["inj_base"] + DateOffset(days=p.HF_lag_dateonly_days),
    )
    out["inj_start_local"] = out["decay_start_local"]
    out["inj_end_local"] = out["inj_start_local"] + pd.Timedelta(days=p.HF_Tmax_days)
    return out

def hf_present_line_window(g: pd.DataFrame) -> dict:
    """
    Given a group (per well) of present rows with Expected Start/End date,
    return a dict with min start, max end + HF_Tmax_days.
    """
    p = PARAMS
    start = g["Expected Start Date"].min()
    end = g["Expected End Date"].max()
    return {
        "inj_start_local": start,
        "decay_start_local": end,
        "inj_end_local": end + pd.Timedelta(days=p.HF_Tmax_days),
    }

def wd_window(df: pd.DataFrame) -> pd.DataFrame:
    """Monthlies: start at period start; delay by WD_delay_months; Tmax window."""
    p = PARAMS
    out = df.copy()
    out["inj_start_local"] = out["yearmonth"].dt.to_period("M").dt.to_timestamp()
    out["decay_start_local"] = out["inj_start_local"] + DateOffset(months=p.WD_delay_months)
    out["inj_end_local"] = out["inj_start_local"] + pd.Timedelta(days=p.WD_Tmax_days)
    return out

def prod_window(df: pd.DataFrame) -> pd.DataFrame:
    """Production start: open window from status_eff to 'now' (as in original)."""
    out = df.copy()
    out["inj_start_local"] = out["status_eff"]
    out["decay_start_local"] = out["inj_start_local"]
    out["inj_end_local"] = pd.Timestamp.utcnow()
    return out

