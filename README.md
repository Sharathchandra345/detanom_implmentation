# DetAnom: Detecting Anomalous Database Transactions

**DetAnom** is a system for detecting anomalous SQL queries from insider threats. It profiles application behavior and flags or blocks unauthorized database access.

## Components

* **Profile Builder** (`profile_builder.py`): Generates query signatures and constraints from the `salaryAdjustment` program.
* **Anomaly Detector** (`anomaly_detector.py`): Validates runtime queries against the profile using strict or flexible policies.

## Features

* Profiles SQL queries based on input-driven conditions.
* Generates schema-based query signatures.
* Captures execution constraints (e.g., `profit >= 0.5 * investment`).
* Strict: block anomalies; Flexible: flag anomalies.
* Uses in-memory SQLite DB.
* Logs for verification and debugging.

## Usage

### 1. Build Profile

```bash
python profile_builder.py
```

Generates `application_profile.json` with 4 query entries.

### 2. Run Anomaly Detector

```bash
python anomaly_detector.py
```

Checks incoming queries against the profile.

### Example Log

```
INFO - Query passed: SELECT employee_id, salary FROM PersonalInfo WHERE salary > 50000  
ERROR - Anomaly detected: SELECT employee_id FROM PersonalInfo WHERE salary < 1000
```

## Requirements

* Python 3.8+
* SQLite (standard library)

## Project Structure

```
DetAnom/
├── profile_builder.py
├── anomaly_detector.py
├── application_profile.json
└── README.md
```

## Reference

* Paper: *DetAnom: Detecting Anomalous Database Transactions by Insiders*, Hussain et al., 2015

---
