# Nimbus EV Location Scoping

## Overview
This repository contains an end-to-end data pipeline designed to ingest, transform, and analyse EV charging points in Central London, built on Google Cloud Platform.

## Repository Structure

### **Start Here: Nimbus Thought Process & Project Run Through**
**This is the primary document for the review.** It outlines:
* An insight into my logic and thinking around the project.
* Strategic decisions and architectural considerations.
* Commercial value and future production challenges.

---

### `cloud_run_function/`
This folder contains the production code currently deployed to Google Cloud.
* **`main.py`**: The core pipeline script (Extract, Transform, Load).
* **`requirements.txt`**: The dependencies required to run the environment.

### `testing/`
This folder contains the local development work.
* Includes Jupyter notebooks and scripts used for initial data exploration.
* Documents the data cleaning logic and quality checks performed locally before moving to the cloud.

---

## Architecture
**Source:** Open Charge Map API  
**Compute:** Google Cloud Functions
**Storage:** Google BigQuery  
**Visualisation:** Looker Studio
