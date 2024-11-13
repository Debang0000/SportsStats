import requests
from bs4 import BeautifulSoup
import csv
import os
import time

# Define the range of years you want to scrape data for
years = [2023]

# Base URL of the website
base_url = 'https://cfbstats.com'

for year in years:
    print(f"\nProcessing year {year}...")
    
    # Create a directory for the year if it doesn't exist
    directory = str(year)
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    # Access the team index page
    team_index_url = f'{base_url}/{year}/team/index.html'
    response = requests.get(team_index_url)
    if response.status_code != 200:
        print(f"Failed to retrieve team index page for {year}. Status code: {response.status_code}")
        continue  # Skip to the next year if the page cannot be retrieved
    else:
        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')
    
        # Find all team links on the index page
        team_links = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            if f'/{year}/team/' in href and href.endswith('index.html'):
                team_name = a.text.strip()
                team_links.append({'name': team_name, 'href': href})
                print(f"Found team: {team_name} - Link: {href}")
    
        print(f"\nFound {len(team_links)} teams in year {year}.")
    
        # Iterate over each team to scrape the roster
        for team in team_links:
            team_name = team['name']
            team_href = team['href']
            # Extract team ID from the href
            parts = team_href.strip('/').split('/')
            if len(parts) >= 3:
                team_id = parts[2]
            else:
                print(f"Could not extract team ID from {team_href}")
                continue
    
            print(f"\nProcessing team {team_name} (ID: {team_id})...")
    
            # Construct the roster URL
            roster_url = f'{base_url}/{year}/team/{team_id}/roster.html'
            print(f"Roster URL: {roster_url}")
    
            # Send a GET request to the roster page
            response = requests.get(roster_url)
            if response.status_code != 200:
                print(f"Failed to retrieve roster page for {team_name} in {year}. Status code: {response.status_code}")
                continue
    
            soup = BeautifulSoup(response.content, 'html.parser')
    
            # Find the roster table
            table = soup.find('table', {'class': 'team-roster'})
            if not table:
                print(f"No roster table found for {team_name} in {year}.")
                continue
    
            # Extract table headers
            headers = [th.text.strip() for th in table.find('tr').find_all('th')]
            print(f"Headers: {headers}")
    
            # Open a CSV file to write the roster data
            filename = os.path.join(directory, f'{team_name.replace(" ", "_")}_roster.csv')
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(headers)
    
                # Iterate over each row in the table
                for row in table.find_all('tr')[1:]:  # Skip the header row
                    cells = [td.text.strip() for td in row.find_all('td')]
                    print(f"Row data: {cells}")
                    if len(cells) == len(headers):
                        writer.writerow(cells)
                    else:
                        print(f"Skipping row due to mismatch in number of columns.")
            print(f"Roster data saved to {filename}")
    
            # Pause between requests
            time.sleep(1)
