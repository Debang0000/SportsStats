import os
import json
import csv
import time
from urllib.parse import urlparse
from dotenv import load_dotenv
from scrapegraphai.graphs import SmartScraperGraph
from scrapegraphai.utils import prettify_exec_info
from websaver import minify_html
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import psutil  # Used to kill zombie processes

# Load environment variables (API keys and URLs)
load_dotenv()

# Kill zombie chromedriver processes
def kill_zombie_chromedrivers():
    for proc in psutil.process_iter():
        try:
            if proc.name() == "chromedriver":
                proc.kill()
        except psutil.NoSuchProcess:
            pass  # Process already terminated

# Call this function before starting new drivers
kill_zombie_chromedrivers()

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
    service = Service(ChromeDriverManager().install(), timeout=120)  # Increased timeout
    return webdriver.Chrome(service=service, options=chrome_options)

# Function to handle rate limit errors and extract the wait time
def handle_rate_limit_error(message):
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
    driver = None  # Initialize driver as None to ensure cleanup
    while retry_count > 0:
        try:
            driver = init_driver()
            driver.set_page_load_timeout(30)  # Set a 30-second page load timeout
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

            # Get the HTML content after the page is fully loaded
            page_source = driver.page_source

            # Minify the HTML content
            minified_html = minify_html(page_source)

            # Define the scraping task with a specific prompt
            prompt = "Extract all players' names, hometowns, and high schools from the HTML."

            # Configuration for the scraper
            graph_config = {
                "llm": {
                    "api_key": os.getenv("GROQ_API_KEY"),  # Store API key in .env file
                    "model": "groq/llama-3.1-8b-instant",
                    "temperature": 0
                },
                "embeddings": {
                    "model": "ollama/nomic-embed-text",
                    "temperature": 0,
                    "base_url": "http://localhost:11434",
                },
                "headless": True
            }

            # Create the SmartScraperGraph instance
            scraper = SmartScraperGraph(
                prompt=prompt,
                source=minified_html,
                config=graph_config,
            )

            # Run the scraper and get the results
            result = scraper.run()

            # Process the results
            if isinstance(result, dict) and 'players' in result:
                players = result['players']
                domain = urlparse(url).netloc.split('.')[0]
                path_segments = urlparse(url).path.strip('/').split('/')
                year = path_segments[-1]
                csv_filename = f'{domain}_{year}_players_data.csv'

                # Save the data to a CSV file
                with open(csv_filename, mode='w', newline='', encoding='utf-8') as file:
                    writer = csv.writer(file)
                    writer.writerow(["Name", "Hometown", "High School"])
                    for player in players:
                        name = player.get('name', '')
                        hometown = player.get('hometown', "Unknown")
                        high_school = player.get('high_school', "Unknown")
                        writer.writerow([name.strip(), hometown.strip(), high_school.strip()])

            return  # Exit after successfully processing the URL

        except Exception as e:
            if 'rate_limit_exceeded' in str(e) or '429' in str(e):
                if handle_rate_limit_error(str(e)):
                    continue  # Retry without decrementing the retry_count
            else:
                retry_count -= 1
                if retry_count == 0:
                    with open('failed_urls.log', 'a') as log_file:
                        log_file.write(f"Failed to process {url}: {str(e)}\n")
                    return  # Exit after retries are exhausted

        finally:
            if driver:
                try:
                    driver.close()  # Close the current tab/window
                except Exception as e_close:
                    pass  # Ignore any errors during closing
                driver.quit()   # Quit the browser and end the WebDriver session

# Use ThreadPoolExecutor to scrape and process multiple URLs concurrently
with ThreadPoolExecutor(max_workers=10) as executor:  # Adjust max_workers as needed
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
