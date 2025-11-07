#!/bin/env bash

python3 timetable-to-gtfs.py \
  "../input-data/Lagos timetables.xlsx" \
  --sheets "BLUE-Outbound,BLUE-Inbound,RED-Inbound,RED-Outbound" \
  --outdir ../gtfs
