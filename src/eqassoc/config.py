from __future__ import annotations
from dataclasses import dataclass, field, replace
from datetime import datetime
from shapely.geometry import LineString

PLANE_EPSG = 26910
DEFAULT_BATCH = 10_000
DEFAULT_DB_URI = "mysql+pymysql://root@localhost/earthquakes"
# Default earthquakes catalogue/table used for association
DEFAULT_EQ_TABLE = "master_origin"
# Fort St. John operates on Mountain Time without daylight savings.
# Use the corresponding zoneinfo key for conversions.
FORT_ST_JOHN_TZ = "America/Fort_Nelson"

@dataclass(frozen=True)
class Params:
    earth_R_km: float = 6371.0

    # Regions (minimal, envelope as in original)
    KSMMA_poly: LineString = field(default_factory=lambda: LineString(
        [(-121.6, 56.0), (-121.0, 56.0),
         (-121.0, 56.25), (-121.6, 56.25), (-121.6, 56.0)]
    ).envelope)

    # Search radii by type and region (km)
    radius_km: dict = field(default_factory=lambda: {
        "HF":   {"KSMMA": 1.0, "Northern Montney": 3.0},
        "WD":   {"KSMMA": 5.0, "Northern Montney": 10.0},
        "PROD": {"KSMMA": 1.0, "Northern Montney": 3.0},
    })

    # Time windows (days/hours semantics mirror original)
    HF_lag_dateonly_days: int = 1
    HF_lag_datetime_hours: int = 0
    HF_Tmax_days: int = 744        # stage presence tail
    WD_delay_months: int = 1
    WD_Tmax_days: int = 365
    PROD_Tmax_days: int = 365 * 2  # used as “open window” until now
    # Probability at Tmax decays by e^(-time_decay_factor)
    time_decay_factor: float = 2.45

    # Weights (same as original)
    weights: dict = field(default_factory=lambda: {
        "HF": 0.9,
        "WD": 0.1,
        "PROD": 0.05,
        "formation": {"Lower Middle Montney": 0.8, "Other": 0.2},
    })

    # Production cutoff date
    prod_cut: datetime = datetime(2010, 1, 1)

PARAMS = Params()


def update_params(**kwargs):
    """Replace :data:`PARAMS` values based on provided keyword arguments.

    Only keys with non-``None`` values are applied.  This helper allows the
    CLI to override configuration such as time-window parameters without
    mutating the frozen :class:`Params` dataclass directly.
    """
    global PARAMS
    update = {k: v for k, v in kwargs.items() if v is not None}
    if update:
        PARAMS = replace(PARAMS, **update)

