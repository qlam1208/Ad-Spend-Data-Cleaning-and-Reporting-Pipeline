# Advertising Spend Reporting Pipeline (Pandas)

A small data processing project that cleans, aggregates, and generates monthly advertising spend reports from multiple raw CSV exports.

## Overview
- Clean raw advertising spend data exported from Meta reports
- Normalize account IDs, dates, and spend values
- Aggregate daily spend into monthly, account-level reports
- Export clean CSV files for client reporting

## Tech Stack
- Python
- pandas

## Input
- Multiple raw CSV files (daily advertising spend data)
- Account-to-client mapping CSV

## Output
- Cleaned dataset
- Monthly spend report per account (daily columns 1–31 and total)
- Client-specific CSV report
