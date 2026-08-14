# GBIF Data Engineering Pipeline

## Overview

This project is a data engineering pipeline for collecting, transforming,
and analyzing biodiversity data from the GBIF API.

The project is designed as a personal data engineering portfolio,
with a focus on maintainability, data quality, testing, and automation.

## Architecture

```text
GBIF API
   ↓
Extract
   ↓
Raw Data
   ↓
Transform
   ↓
Processed Data
   ↓
DuckDB / SQL
   ↓
Analysis