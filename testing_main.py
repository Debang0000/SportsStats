import os
import json
import csv
import time
from urllib.parse import urlparse
from dotenv import load_dotenv
from scrapegraphai.graphs import SmartScraperGraph
from scrapegraphai.utils import prettify_exec_info
from websaver import minify_html  # Importing minify_html for HTML processing
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

# Load environment variables (API keys and URLs)
load_dotenv()

# Read URLs from url_list.txt
with open('url_list.txt', 'r') as url_file:
    urls = [line.strip() for line in url_file.readlines() if line.strip()]  # Get list of URLs

# Set Chrome options (optional, for headless mode or other configurations)
chrome_options = Options()
chrome_options.add_argument("--headless")  # Run Chrome in headless mode (without a UI)
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

# Initialize Selenium WebDriver using the Service object and ChromeDriverManager
def init_driver():
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

# Function to handle rate limit errors and extract the wait time
def handle_rate_limit_error(message):
    # Example message format:
    # 'Rate limit reached for model ... Please try again in 1m24.14s.'
    match = re.search(r"try again in (\d+m\d+\.\d+s)", message)
    if match:
        wait_time_str = match.group(1)
        minutes, seconds = re.match(r'(\d+)m(\d+\.\d+)s', wait_time_str).groups()
        wait_time = int(minutes) * 60 + float(seconds)
        time.sleep(wait_time)
        return True
    return False

# Define a function to scrape a single URL and return its minified HTML
def scrape_and_process(url):
    retry_count = 3  # Retry 3 times on error
    while retry_count > 0:
        try:
            driver = init_driver()
            driver.get(url)

            # Scroll to the bottom of the page to load all dynamic content
            SCROLL_PAUSE_TIME = 3
            last_height = driver.execute_script("return document.body.scrollHeight")

            while True:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(SCROLL_PAUSE_TIME)
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height

            # Get the HTML content after the page is fully loaded and close the driver
            page_source = driver.page_source
            driver.quit()

            # Minify the HTML content
            minified_html = minify_html(page_source)

            # Define the configuration for the AI scraper with the Groq model
            graph_config = {
                "llm": {
                    "api_key": "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",  # API key for Groq
                    "model": "groq/llama-3.1-8b-instant",  # Groq model for scraping
                    "temperature": 0  # Set to 0 for deterministic output
                },
                "embeddings": {
                    "model": "ollama/nomic-embed-text",
                    "temperature": 0,
                    "base_url": "http://localhost:11434", 
                },
                "headless": True
            }

            # Define the scraping task with a specific prompt
            prompt = "Extract all players' names, hometowns, and high schools from the HTML."

            # Create the SmartScraperGraph instance and run it with the minified HTML
            scraper = SmartScraperGraph(
                prompt=prompt,
                source=minified_html,  # Use the minified HTML content from RAM
                config=graph_config,
            )

            # Run the scraper and get the results
            result = scraper.run()

            # Check if the result has data and save it into a CSV file
            if isinstance(result, dict) and 'players' in result:
                players = result['players']  # Extract list of players

                # Generate a CSV filename based on the URL
                domain = urlparse(url).netloc.split('.')[0]  # Corrected to extract the first part of the domain
                path_segments = urlparse(url).path.strip('/').split('/')
                year = path_segments[-1]
                csv_filename = f'{domain}_{year}_players_data.csv'

                # Open a CSV file for writing
                with open(csv_filename, mode='w', newline='', encoding='utf-8') as file:
                    writer = csv.writer(file)

                    # Write the header row
                    writer.writerow(["Name", "Hometown", "High School"])

                    # Write each player's information
                    for player in players:
                        name = player.get('name', '')
                        hometown = player.get('hometown', "Unknown")
                        high_school = player.get('high_school', "Unknown")

                        # Write the row to the CSV
                        writer.writerow([name.strip(), hometown.strip(), high_school.strip()])

                return  # Successfully processed this URL

            retry_count = 0  # Exit retry loop if no error

        except Exception as e:
            if 'rate_limit_exceeded' in str(e):
                # Handle rate limit error
                if handle_rate_limit_error(str(e)):
                    retry_count = 3  # Reset retry count and retry this URL after waiting
                    continue  # Retry the same URL
            else:
                retry_count -= 1  # Decrement retry count on other errors
                if retry_count == 0:
                    # Log failed URL
                    with open('failed_urls.log', 'a') as log_file:
                        log_file.write(f"Failed to process {url}: {str(e)}\n")
                    return  # Exit after retries are exhausted

# Use ThreadPoolExecutor to scrape and process multiple URLs concurrently
with ThreadPoolExecutor(max_workers=5) as executor:  # Adjust max_workers as needed
    future_to_url = {executor.submit(scrape_and_process, url): url for url in urls}

    # Gather results as they complete
    for future in as_completed(future_to_url):
        url = future_to_url[future]
        try:
            future.result()
        except Exception as exc:
            # If there's a problem that isn't rate-limit related, it should be logged
            with open('failed_urls.log', 'a') as log_file:
                log_file.write(f"Failed to complete {url}: {str(exc)}\n")
