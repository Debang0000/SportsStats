import os
import csv
import glob
import time
from collections import defaultdict
from geopy.geocoders import Nominatim
from geopy.distance import geodesic

# Initialize geolocator with a user agent
geolocator = Nominatim(user_agent="cfb_distance_calculator")

# Cache for geocoded locations to avoid redundant requests
location_cache = {}

# Function to geocode a location
def geocode_location(location):
    if not location or location.strip() == '-' or location.strip() == '':
        return None
    location = location.strip()
    if location in location_cache:
        return location_cache[location]
    try:
        geo_location = geolocator.geocode(location, timeout=10)
        if geo_location:
            coordinates = (geo_location.latitude, geo_location.longitude)
            location_cache[location] = coordinates
            return coordinates
        else:
            return None
    except Exception as e:
        return None

# Load team locations from team_locations.csv
team_locations = {}
with open('team_locations.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        team = row.get('Team')
        location = row.get('Location')
        if team and location and location.strip() != '-':
            team = team.strip()
            location = location.strip()
            team_locations[team] = location

# List of years to process
years = [2018, 2019, 2020, 2021, 2022]

# Lists to store detailed and average distances
detailed_distances = []
average_distances = []

for year in years:
    print(f"\nProcessing rosters for year {year}...")
    directory = str(year)
    if not os.path.exists(directory):
        print(f"Directory {directory} does not exist. Skipping year {year}.")
        continue

    # Get a list of all roster CSV files in the directory
    csv_files = glob.glob(os.path.join(directory, '*_roster.csv'))
    print(f"Found {len(csv_files)} roster files.")

    for csv_file in csv_files:
        team_name = os.path.basename(csv_file).replace('_roster.csv', '').replace('_', ' ')
        print(f"\nProcessing team {team_name}...")
        player_distances = []
        total_players = 0
        missing_hometown_count = 0

        # Get the university location for the team
        university_location = team_locations.get(team_name)
        if not university_location:
            print(f"University location for '{team_name}' not found in team_locations.csv.")
            continue

        # Geocode the university location
        university_coords = geocode_location(university_location)
        if not university_coords:
            print(f"Could not geocode university location for '{team_name}': {university_location}")
            continue

        # Read the roster CSV file
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                total_players += 1
                player_name = row.get('Name', 'Unknown')
                hometown = row.get('Hometown')
                if not hometown or hometown.strip() == '-' or hometown.strip() == '':
                    missing_hometown_count += 1
                    continue

                # Geocode the player's hometown
                hometown_coords = geocode_location(hometown)
                if not hometown_coords:
                    missing_hometown_count += 1
                    continue

                # Calculate the distance
                distance = geodesic(hometown_coords, university_coords).kilometers
                player_distances.append(distance)

                # Add to detailed distances
                detailed_distances.append({
                    'Year': year,
                    'Team': team_name,
                    'Player': player_name,
                    'Hometown': hometown,
                    'Distance (km)': round(distance, 2)
                })

                # Pause to respect API rate limits
                time.sleep(1)

        if player_distances:
            average_distance = sum(player_distances) / len(player_distances)
            average_distances.append({
                'Year': year,
                'Team': team_name,
                'Average Distance (km)': round(average_distance, 2),
                'Player Count': total_players,
                'Players with Hometown': len(player_distances),
                'Missing Hometown': missing_hometown_count
            })
            print(f"Average distance for '{team_name}' in {year}: {average_distance:.2f} km")
        else:
            print(f"No valid player distances calculated for '{team_name}' in {year}.")
            average_distances.append({
                'Year': year,
                'Team': team_name,
                'Average Distance (km)': None,
                'Player Count': total_players,
                'Players with Hometown': 0,
                'Missing Hometown': missing_hometown_count
            })

        # Pause between teams
        time.sleep(1)

# Write the detailed distances to a CSV file
detailed_output_file = 'detailed_player_distances.csv'
with open(detailed_output_file, 'w', newline='', encoding='utf-8') as f:
    fieldnames = ['Year', 'Team', 'Player', 'Hometown', 'Distance (km)']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(detailed_distances)

# Write the average distances to a CSV file
average_output_file = 'average_team_distances.csv'
with open(average_output_file, 'w', newline='', encoding='utf-8') as f:
    fieldnames = ['Year', 'Team', 'Average Distance (km)', 'Player Count', 'Players with Hometown', 'Missing Hometown']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(average_distances)

print(f"\nDetailed distances saved to {detailed_output_file}")
print(f"Average distances saved to {average_output_file}")
