# Weather Data Pipeline

## Overview
This project collects, processes, and stores weather data for multiple cities. Data is fetched from the OpenWeatherMap API, cleaned, and then stored in MongoDB and Snowflake for further analysis.

---

## Components

### 1. Data Extraction
- Fetch current weather data using OpenWeatherMap API.
- Supports parallel requests with rate limiting to respect API limits.
- Handles errors and logs failed city fetches.

### 2. Data Cleaning
- Cleans and normalizes raw API data.
- Extracts relevant fields such as temperature, humidity, wind speed, and timestamps.

### 3. Data Storage
- Stores raw and cleaned data in MongoDB.
- Loads cleaned data into Snowflake, creating the table if necessary.
- Avoids duplicates by using unique indexes and merge logic.

---

## Setup

### Prerequisites
- Python 3.x
- MongoDB instance and credentials
- Snowflake account and credentials
- OpenWeatherMap API key

### Install dependencies
```bash
pip install pandas pymongo requests snowflake-connector-python
