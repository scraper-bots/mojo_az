# Mojo.az User Scraper - OPTIMIZED

High-performance web scraper for extracting user data from mojo.az with phone validation, crash recovery, and auto-save functionality.

## Features

### Speed Optimizations
- **200 concurrent requests** (4x faster than before)
- **10s timeout** for fast failure on non-responsive pages
- **Connection pooling** with 300 connection limit
- **Large batch processing** (5000 users per batch)
- **Optimized HTML parsing** strategy

### Reliability Features
- **Auto-save checkpoints** every 2000 users
- **Crash recovery** - automatically resumes from last checkpoint
- **Progress tracking** - saves last processed user ID
- **Graceful shutdown** - saves progress on Ctrl+C

### Data Quality
- **Phone validation** - Only saves valid Azerbaijan phone numbers
- **Multi-format export** - CSV, XLSX, and JSON
- **Comprehensive logging** - Real-time statistics and progress

## Phone Number Validation

Only phone numbers that meet ALL criteria are saved:
1. Extract all non-numeric characters
2. Take last 9 digits
3. First 2 digits must be: 10, 50, 51, 55, 60, 70, 77, or 99
4. 3rd digit cannot be 0 or 1

## Installation

```bash
pip install -r requirements.txt
```

## Usage

### Quick Start

```bash
python mojo_scraper.py
```

This will scrape users 1-47000 and export to:
- `mojo_users.csv`
- `mojo_users.xlsx`
- `mojo_users.json`

### Resume After Crash

The scraper automatically detects and loads `scraper_checkpoint.json` if it exists. Just run:

```bash
python mojo_scraper.py
```

### Test Small Batch

```bash
python test_scraper.py
```

### Custom Configuration

Edit `mojo_scraper.py` main() function:

```python
START_ID = 1              # Starting user ID
END_ID = 47000            # Ending user ID
MAX_CONCURRENT = 200      # Concurrent requests (optimized)
BATCH_SIZE = 5000         # Users per batch (optimized)
SAVE_EVERY = 2000         # Checkpoint frequency
```

## Output Format

Each valid user record contains:

```json
{
  "user_id": 46823,
  "name": "Mustafayeva Nəzrin",
  "phone": "708533207",
  "registration_date": "Bugün, 00:39",
  "last_seen_date": "Bugün, 00:43",
  "listing_count": 0,
  "url": "https://mojo.az/az/users/46823",
  "scraped_at": "2025-10-30T12:34:56"
}
```

## Performance

**Test Results (51 users):**
- Total time: ~20 seconds
- Valid users found: 27 (53% success rate)
- Speed: ~2.5 users/second

**Estimated Time for Full Scrape (47,000 users):**
- At 2.5 users/sec: ~5.2 hours
- Optimized estimate: ~3-4 hours

**Optimizations:**
- Concurrent requests: 200 (4x increase)
- Batch processing: 5000 users per batch
- Connection timeout: 10s (fast fail)
- Connection pooling: 300 limit

## Logging

Logs are saved to `scraper.log` and printed to console:

```
2025-10-30 12:34:56 - INFO - Starting scrape: 1 to 47000 (47000 users)
2025-10-30 12:34:57 - INFO - ✓ User 1: John Doe - 505551234
2025-10-30 12:34:58 - INFO - ✗ User 2: HTTP 404
2025-10-30 12:34:59 - INFO - Valid users saved: 1500
```

## Statistics

After completion, you'll see:

```
============================================================
SCRAPING COMPLETE
============================================================
Total processed: 47000
Valid users saved: 15234
Failed requests: 12500
No phone found: 10000
Invalid phone: 9266
============================================================
```

## Crash Recovery

The scraper automatically saves progress:
- Every 2000 users processed
- After each batch completes
- On manual interrupt (Ctrl+C)
- On unexpected errors

To resume, simply run the scraper again - it will detect `scraper_checkpoint.json` and continue from where it left off.

## Output Files

After scraping, you'll get:

1. **mojo_users.csv** - CSV format
2. **mojo_users.xlsx** - Excel format
3. **mojo_users.json** - JSON format
4. **scraper_checkpoint.json** - Recovery checkpoint (auto-generated)
5. **scraper.log** - Detailed logs

## Files

- `mojo_scraper.py` - Main scraper (optimized)
- `phone_validator.py` - Azerbaijan phone number validation
- `test_scraper.py` - Test script for validation
- `requirements.txt` - Python dependencies

## Notes

- Only valid phone numbers are saved (one row = one valid user)
- Invalid or missing phones are NOT saved
- Automatically resumes after crash
- Progress tracked and logged in real-time
- Safe to interrupt (Ctrl+C) - progress is saved
