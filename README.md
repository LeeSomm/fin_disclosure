# Congressional Trading Monitor

A Python-based system for monitoring and tracking Congressional financial 
disclosure filings, with automated extraction of trading data and notification capabilities.

## Overview

This project automatically scrapes Congressional (Representatives only at the moment) 
financial disclosure filings from the House Clerk's website, extracts trading 
transaction data from PDF documents, and provides notifications for new trading 
activities by members of Congress. 

## Features

- **Automated Filing Scraping**: Monitors the House Clerk's financial disclosure database for new filings
- **PDF Processing**: Extracts trading transaction data from disclosure PDF documents
- **Data Management**: Centralized JSON-based storage for filings and trading data
- **Status Tracking**: Manages processing status of filings to avoid duplicate work
- **Notification System**: Sends push notifications for new trading activities via the Bark API
- **Daily Automation**: Designed for scheduled daily runs to capture new filings (bash script not included)

## Project Structure

```
├── daily_run.py              # Main orchestration script for daily processing
├── filing_scraper.py         # Scrapes Congressional financial disclosure filings
├── transaction_extractor.py  # Extracts trading data from PDF documents
├── data_manager.py           # Centralized data access and JSON file operations
├── filing_status_manager.py  # Tracks processing status of filings
├── notification_manager.py   # Handles notification delivery
├── delete_filing.py          # Utility for removing filings from the database
└── data/
    ├── congress_filings.json # Storage for scraped filing metadata
    └── trading_data.json     # Extracted trading transaction data
```

## Key Components

### DailyRun
The main orchestration class that coordinates the entire pipeline:
1. Scrapes latest filings from the House Clerk website
2. Updates the trading database with new filings
3. Identifies pending filings that need processing
4. Processes PDF documents to extract trading data
5. Sends notifications when new filings are processed

### FilingScraper
Handles web scraping of the Congressional financial disclosure database:
- Fetches filing metadata from the House Clerk website
- Supports both incremental and full scraping modes
- Maintains persistent storage of filing information

### TradingDataExtractor
Processes PDF documents to extract trading transaction data:
- Downloads and parses PDF disclosure forms
- Extracts transaction details (stocks, dates, amounts, etc.)
- Handles various PDF formats and layouts
- Categorizes transactions by owner (member, spouse, dependent child)

### DataManager
Provides centralized data access with:
- Atomic file operations
- Consistent error handling
- Data validation
- JSON file management

### FilingStatusManager
Tracks the processing status of filings to:
- Identify which filings need processing
- Prevent duplicate work
- Manage the processing queue

### NotificationManager
Handles delivery of notifications for:
- New trading activities
- Processing summaries
- Error alerts

## Setup

1. Install required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Create a `.env` file with necessary environment variables:
   ```
   DATA_DIR=./data
   # Add other configuration variables as needed
   ```

3. Ensure the `data/` directory exists for storing JSON files

## Usage

### Daily Processing
Run the main daily processing pipeline:
```python
from daily_run import DailyRun

daily_runner = DailyRun()
result = daily_runner.run()
```

### Manual Scraping
Scrape filings independently:
```python
from filing_scraper import FilingScraper

scraper = FilingScraper()
results = scraper.update_data(force_full_scrape=False)
```

### Extract Trading Data
Process specific PDF files:
```python
from transaction_extractor import TradingDataExtractor

extractor = TradingDataExtractor()
transactions = extractor.extract_trading_data("path/to/filing.pdf")
```

## Data Storage

- **congress_filings.json**: Contains metadata for all scraped filings
- **trading_data.json**: Stores extracted trading transaction data

## Dependencies

- `requests`: HTTP requests for web scraping
- `beautifulsoup4`: HTML parsing
- `pdfplumber`: PDF text extraction
- `tenacity`: Retry logic for robust operations
- `python-dotenv`: Environment variable management
- `aiohttp`: Async HTTP for notifications

## Next Steps

1. Enforce better typing and validation with Pydantic
2. Introduce unit testing with pytest
3. Implement process for Senate financial disclosures

## License

This project is for educational and transparency purposes to monitor Congressional 
trading activities in accordance with public disclosure requirements.
