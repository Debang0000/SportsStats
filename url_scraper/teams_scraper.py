import requests
from bs4 import BeautifulSoup
from googlesearch import search
import time

# Step 1: Scrape the Wikipedia page to get the FBS team names
wiki_url = "https://en.wikipedia.org/wiki/List_of_NCAA_Division_I_FBS_football_programs"
response = requests.get(wiki_url)
soup = BeautifulSoup(response.text, 'html.parser')

# Find the table with the list of FBS teams
table = soup.find('table', {'class': 'wikitable'})

# Extract team names from the table
teams = []
for row in table.find_all('tr')[1:]:
    team_name = row.find_all('td')[0].text.strip()
    teams.append(team_name)

# Function to find the roster URL for a specific year
def find_roster_url(team_name, year):
    query = f"{team_name} football roster {year}"
    try:
        for result in search(query, num=1, stop=1, pause=2):
            return result
    except Exception as e:
        print(f"Error performing Google search for {team_name}: {e}")
    return None

# Function to generate URLs for 2018-2022 based on the 2018 URL
def generate_roster_urls(base_url):
    urls = []
    for year in range(2018, 2023):
        url = base_url.replace('2018', str(year))
        urls.append(url)
    return urls

# Open the file in append mode so we can write the URLs as they are found
file_path = 'fbs_team_roster_urls.txt'
with open(file_path, 'a') as file:  # Open file in append mode

    # Iterate over all teams
    for team in teams:
        try:
            # Find the 2018 roster URL for the team
            base_url = find_roster_url(team, 2018)
            if base_url:
                # Generate URLs for 2018-2022
                roster_urls = generate_roster_urls(base_url)

                # Write the team and its URLs to the file immediately
                file.write(f"{team}:\n")
                for url in roster_urls:
                    file.write(f"{url}\n")
                file.write("\n")  # Add a newline between teams
                print(f"Successfully wrote URLs for {team}")

            else:
                print(f"No valid roster URL found for {team}.")
            
            time.sleep(2)  # Add a delay to avoid overwhelming the search engine

        except Exception as e:
            print(f"Error finding roster for {team}: {e}")

print(f"URLs have been saved to {file_path}")
