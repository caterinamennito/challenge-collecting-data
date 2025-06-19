# Real Estate Scraper for Immoweb

This Python project is a web scraper for extracting property listings from Immoweb.be. It collects detailed real estate information and stores the data into a CSV file.

## Features

Fetches listings for apartments, houses, villas, etc.
Parses detailed attributes including price, area, EPB class, number of rooms, and more.
Writes clean structured output in CSV format.

🚀 Usage
1. Install dependencies:

`pip install -r requirements.txt`

2. Run the scraper:

`python main.py`


## Overview

There are several methods to fetch data in different ways (Selenium, Playwright, Beaurifulsoup). I eventually used beautifulsoup, but left the other methods in there
