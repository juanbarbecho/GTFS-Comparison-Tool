# GTFS Pick Comparison Tool
A Python + SQL workflow to compare quarterly GTFS schedule changes and identify added/removed stops, route modifications, and schedule differences. 

# Project Overview
This project was developed during my internship at the MTA to streamline the process of comparing quarterly GTFS "picks." Planners can use it to quickly detect service changes and track trends in ridership impact. 

# Contents
- /notebooks
  - StopList.ipynb – Identifies stop-level changes between any two picks.
  - Schedule.ipynb - Compares trip frequencies by route/direction/hour.
  - RouteSummary.ipynb - Summarizes service changes by time-of-day categories.
  - VariantComparison.ipynb - Algorithm for detecting route 'variant' matches.
- /sample output
  - includes screenshots and files of output
- /sql
  - includes queries used to extract and transform data
 
# Notes
Original code was connected to the MTA Datalake via secure credentials. This repo shows code structure and outputs. 
Next Step: Load GTFS data manually to make notebooks fully reproducible.
