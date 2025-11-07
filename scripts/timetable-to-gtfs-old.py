#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Convert a wide timetable spreadsheet into GTFS trips.txt and stop_times.txt.

Enhancements per your spec:
- route_id is read from the cell to the right of "Route" (e.g., A1 -> B1).
- default direction_id is read from the cell to the right of "Direction ID" (e.g., A2 -> B2).
- trip_id values start with the route name/id (e.g., "blue_00001").

Layout assumptions:
- Header row of stop names, and the row right below contains "Arrive"/"Depart".
- Each subsequent row is a trip instance.
- Optional metadata columns (e.g., "Calendar service", "Headsign", "Direction ID") may be to the left of the first stop column.

Usage:
    python timetable_to_gtfs.py timetable.xlsx \
      --sheet "Sheet1" \
      --default_service_id WEEKDAY \
      --outdir ./gtfs_out
"""

import argparse
import os
import re
import sys
from typing import List, Tuple, Dict, Optional

import pandas as pd


ARRIVE_LABELS = {"arrive", "arrival", "arr", "arr."}
DEPART_LABELS = {"depart", "departure", "dep", "dep."}


def slugify(value: str) -> str:
    value = str(value or "").strip()
    value = re.sub(r"\([^)]*\)", "", value)
    value = re.sub(r"[^A-Za-z0-9]+", "_", value).strip("_")
    return value.lower() or "stop"


def excel_fraction_to_hms(val: float) -> str:
    secs = int(round(float(val) * 86400))
    hours = secs // 3600
    secs %= 3600
    minutes = secs // 60
    seconds = secs % 60
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def parse_time_cell(x) -> Optional[str]:
    if pd.isna(x):
        return None
    if isinstance(x, (pd.Timestamp, )):
        return f"{x.hour:02d}:{x.minute:02d}:{x.second:02d}"
    if isinstance(x, (int, float)):
        if 0 <= float(x) <= 3:  # Excel day-fraction (0–3 days)
            return excel_fraction_to_hms(float(x))
        secs = int(round(float(x)))
        h = secs // 3600
        m = (secs % 3600) // 60
        s = secs % 60
        return f"{h:02d}:{m:02d}:{s:02d}"
    s = str(x).strip()
    if not s:
        return None
    m = re.match(r"^\s*(\d{1,3}):(\d{1,2})(?::(\d{1,2}))?\s*$", s)
    if m:
        h = int(m.group(1))
        mi = int(m.group(2))
        se = int(m.group(3)) if m.group(3) else 0
        return f"{h:02d}:{mi:02d}:{se:02d}"
    try:
        ts = pd.to_datetime(s, errors="raise")
        return f"{ts.hour:02d}:{ts.minute:02d}:{ts.second:02d}"
    except Exception:
        return None


def detect_header_rows(df: pd.DataFrame) -> Tuple[int, int, List[Tuple[int, str, str]]]:
    """Find stop-name header row and the Arrive/Depart subheader row."""
    subheader_idx = None
    for i in range(min(8, len(df))):
        labels = set(str(x).strip().lower() for x in df.iloc[i].tolist())
        if (labels & ARRIVE_LABELS) or (labels & DEPART_LABELS):
            subheader_idx = i
            break
    if subheader_idx is None:
        raise ValueError("Could not find a subheader row with 'Arrive'/'Depart'.")
    stopname_idx = subheader_idx - 1
    if stopname_idx < 0:
        raise ValueError("Stop-name header row not found above Arrive/Depart row.")
    stop_columns = []
    for c in range(df.shape[1]):
        kind_raw = str(df.iat[subheader_idx, c]).strip().lower()
        if kind_raw in ARRIVE_LABELS or kind_raw in DEPART_LABELS:
            raw_stopname = str(df.iat[stopname_idx, c]).strip()
            kind = "arrive" if kind_raw in ARRIVE_LABELS else "depart"
            stop_columns.append((c, raw_stopname, kind))
    if not stop_columns:
        raise ValueError("Found Arrive/Depart row, but no stop columns were identified.")
    return stopname_idx, subheader_idx, stop_columns


def find_value_to_right(df: pd.DataFrame, label: str, search_rows: int = 6) -> Optional[str]:
    """Return the cell immediately to the right of a cell that equals `label` (case-insensitive)."""
    label_lower = label.strip().lower()
    for r in range(min(search_rows, len(df))):
        for c in range(df.shape[1] - 1):
            cell = str(df.iat[r, c]).strip().lower()
            if cell == label_lower:
                right = df.iat[r, c + 1]
                return None if pd.isna(right) else str(right).strip()
    return None


def direction_coerce(val: Optional[str]) -> Optional[int]:
    if val is None:
        return None
    if val == "Onward":
        return 0
    if val == "Return":
        return 1
    try:
        return int(float(val))
    except Exception:
        return None


def build_trips_and_stop_times(
    df: pd.DataFrame,
    route_id: Optional[str] = None,
    default_service_id: str = "WEEKDAY",
    default_direction_id: Optional[int] = None,
    trip_id_prefix: Optional[str] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # Pull route_id / default direction from the top-left labels
    detected_route = find_value_to_right(df, "Route") or route_id
    direction_id = direction_coerce(find_value_to_right(df, "Direction ID"))
    if direction_id is None:
        direction_id = default_direction_id
    route_id = detected_route or route_id or "ROUTE"

    # Trip IDs should start with the route name/id
    if trip_id_prefix is None:
        trip_id_prefix = f"{slugify(route_id)}_"

    stopname_idx, subheader_idx, stop_columns = detect_header_rows(df)

    # Metadata columns lie left of the first time column
    first_time_col = min(c for c, _, _ in stop_columns)
    header_block = df.iloc[:subheader_idx + 1, :first_time_col]
    meta_cols = list(header_block.columns)

    def match_meta(name: str) -> Optional[int]:
        low = name.strip().lower()
        for c in header_block.columns:
            vals = [str(v).strip().lower() for v in header_block[c].tolist()]
            if low in vals:
                return c
        return None

    col_service = match_meta("Calendar service") or match_meta("service_id")
    col_headsign = match_meta("Headsign") or match_meta("trip_headsign")
    col_direction = match_meta("Direction ID") or match_meta("direction_id")

    # Build canonical stop list / ids
    stop_order: List[str] = []
    seen = set()
    for c, raw_stop, kind in stop_columns:
        if raw_stop not in seen:
            seen.add(raw_stop)
            stop_order.append(raw_stop)
    stop_id_map = {name: slugify(name) for name in stop_order}

    trips = []
    stop_times_rows = []
    data_start_row = subheader_idx + 1
    trip_counter = 1

    for r in range(data_start_row, df.shape[0]):
        row = df.iloc[r]
        # gather times by stop
        times_by_stop: Dict[str, Dict[str, Optional[str]]] = {s: {"arrive": None, "depart": None} for s in stop_order}
        for c, raw_stop, kind in stop_columns:
            val = row.iloc[c]
            t = parse_time_cell(val)
            if t:
                times_by_stop[raw_stop][kind] = t

        non_null_times = sum(
            1 for s in stop_order for k in ("arrive", "depart") if times_by_stop[s][k]
        )
        if non_null_times < 2:
            continue

        # service, direction, headsign
        service_id = default_service_id
        if col_service is not None:
            v = row.iloc[col_service]
            if not pd.isna(v):
                service_id = str(v).strip() or service_id

        trip_headsign = None
        if col_headsign is not None:
            v = row.iloc[col_headsign]
            if not pd.isna(v):
                trip_headsign = str(v).strip() or None

        trip_id = f"{trip_id_prefix}{trip_counter:05d}"
        trip_counter += 1

        trip_record = {"route_id": route_id, "service_id": service_id, "trip_id": trip_id}
        if direction_id is not None:
            trip_record["direction_id"] = direction_id
        if trip_headsign:
            trip_record["trip_headsign"] = trip_headsign
        trips.append(trip_record)

        # emit stop_times
        seq = 1
        for stop_name in stop_order:
            arr = times_by_stop[stop_name]["arrive"]
            dep = times_by_stop[stop_name]["depart"]
            if not arr and dep:
                arr = dep
            if not dep and arr:
                dep = arr
            if not arr and not dep:
                continue
            stop_times_rows.append(
                {
                    "trip_id": trip_id,
                    "arrival_time": arr,
                    "departure_time": dep,
                    "stop_id": stop_id_map[stop_name],
                    "stop_sequence": seq,
                }
            )
            seq += 1

        if seq <= 2:
            trips.pop()
            stop_times_rows = [st for st in stop_times_rows if st["trip_id"] != trip_id]

    trips_df = pd.DataFrame(trips)
    stop_times_df = pd.DataFrame(stop_times_rows).sort_values(
        ["trip_id", "stop_sequence"]
    ).reset_index(drop=True)

    return trips_df, stop_times_df


def main():
    ap = argparse.ArgumentParser(description="Convert a wide timetable spreadsheet into GTFS trips.txt and stop_times.txt.")
    ap.add_argument("input", help="Input .xlsx/.xls or .csv")
    ap.add_argument("--sheet", help="Sheet name (Excel). If omitted, first sheet.")
    ap.add_argument("--default_service_id", default="WEEKDAY", help="Fallback when 'Calendar service' column missing/blank.")
    ap.add_argument("--default_direction_id", type=int, default=None, help="Fallback direction when not detected at top or per-row.")
    ap.add_argument("--outdir", default=".", help="Output directory.")
    args = ap.parse_args()

    # Load input
    if not os.path.exists(args.input):
        print(f"Input not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    if args.input.lower().endswith((".xlsx", ".xls")):
        df = pd.read_excel(args.input, sheet_name=args.sheet if args.sheet else 0, header=None)
    elif args.input.lower().endswith(".csv"):
        df = pd.read_csv(args.input, header=None)
    else:
        print("Unsupported input type. Use .xlsx, .xls, or .csv", file=sys.stderr)
        sys.exit(1)

    trips_df, stop_times_df = build_trips_and_stop_times(
        df,
        route_id=None,  # read from "Route" label by default
        default_service_id=args.default_service_id,
        default_direction_id=args.default_direction_id,
        trip_id_prefix=None,  # auto: route_id_
    )

    os.makedirs(args.outdir, exist_ok=True)
    trips_path = os.path.join(args.outdir, "trips.txt")
    stop_times_path = os.path.join(args.outdir, "stop_times.txt")

    trips_cols = ["route_id", "service_id", "trip_id"]
    if "direction_id" in trips_df.columns:
        trips_cols.append("direction_id")
    if "trip_headsign" in trips_df.columns:
        trips_cols.append("trip_headsign")

    trips_df[trips_cols].to_csv(trips_path, index=False)
    stop_times_df[["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"]].to_csv(stop_times_path, index=False)

    print(f"Wrote {trips_path} ({len(trips_df)} trips)")
    print(f"Wrote {stop_times_path} ({len(stop_times_df)} stop_times)")


if __name__ == "__main__":
    main()
