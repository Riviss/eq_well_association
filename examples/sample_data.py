import pandas as pd
from eqassoc.regions import assign_region


def load_raw_sample_data():
    """Return small in-memory DataFrames for earthquakes and activities."""
    # Earthquake catalog with times already localized to Pacific
    eq = pd.DataFrame(
        {
            "quake_id": [1, 2],
            "latitude": [56.10, 56.30],
            "longitude": [-121.30, -121.20],
            "depth_km": [3.0, 5.0],
            "time_local": pd.to_datetime(["2023-02-15", "2023-02-20"]),
        }
    )
    eq = assign_region(eq)

    # Raw HF stage record
    hf = pd.DataFrame(
        {
            "stage_id": [10],
            "well_id": ["A1"],
            "pad_id": ["P1"],
            "formation": ["Lower Middle Montney"],
            "latitude": [56.11],
            "longitude": [-121.31],
            "depth_m": [2500],
            "datetime": pd.to_datetime(["2023-01-01"]),
            "date": [pd.Timestamp("2023-01-01").date()],
        }
    )
    hf["depth_km"] = hf["depth_m"] / 1000.0

    # Raw water disposal record (monthly)
    wd = pd.DataFrame(
        {
            "well_id": ["W1"],
            "latitude": [56.09],
            "longitude": [-121.29],
            "depth_m": [1500],
            "yearmonth": pd.to_datetime(["2022-12-01"]),
        }
    )
    wd["depth_km"] = wd["depth_m"] / 1000.0

    # Raw production record
    prod = pd.DataFrame(
        {
            "well_id": ["P2"],
            "latitude": [56.12],
            "longitude": [-121.32],
            "status_eff": pd.to_datetime(["2022-01-01"]),
        }
    )
    prod["depth_km"] = 2.0

    return eq, hf, wd, prod
