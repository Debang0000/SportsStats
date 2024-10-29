import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Read the detailed distances data
data_file = 'detailed_player_distances.csv'
df = pd.read_csv(data_file)

# Set up Seaborn style
sns.set(style='whitegrid')

# Get the list of years and teams
years = df['Year'].unique()
teams = df['Team'].unique()

# Create output directory for plots
plot_dir = 'distance_plots'
if not os.path.exists(plot_dir):
    os.makedirs(plot_dir)

for year in years:
    df_year = df[df['Year'] == year]
    teams_in_year = df_year['Team'].unique()
    print(f"\nCreating plots for year {year}...")

    # Create a directory for the year
    year_dir = os.path.join(plot_dir, str(year))
    if not os.path.exists(year_dir):
        os.makedirs(year_dir)

    for team in teams_in_year:
        df_team = df_year[df_year['Team'] == team]
        distances = df_team['Distance (km)']
        if distances.empty:
            print(f"No distance data for team {team} in year {year}. Skipping plot.")
            continue

        # Set up the plot
        plt.figure(figsize=(10, 6))
        sns.histplot(distances, bins=20, kde=True, color='skyblue')
        plt.title(f'Distance Distribution for {team} ({year})')
        plt.xlabel('Distance from Hometown to University (km)')
        plt.ylabel('Number of Players')

        # Save the plot
        team_plot_dir = os.path.join(year_dir)
        if not os.path.exists(team_plot_dir):
            os.makedirs(team_plot_dir)
        plot_filename = f"{team.replace(' ', '_')}_{year}_distance_distribution.png"
        plot_path = os.path.join(team_plot_dir, plot_filename)
        plt.savefig(plot_path)
        plt.close()
        print(f"Plot saved to {plot_path}")
