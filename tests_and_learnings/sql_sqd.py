import os
import sqlite3
import pandas as pd
from datetime import datetime

# Directory containing the Squaredance CSV files
squaredance_dir = "/Users/chandnimelwani/Documents/MMAI/Capstone/code_repo/capstone-mas/data/data_transformations/bonafide/Affiliate Network Platform/Squaredance"

# Connect to SQLite database
conn = sqlite3.connect('bonafide_master.db')

# Initialize an empty DataFrame to store the combined data
combined_df = pd.DataFrame()

# Process each CSV file in the Squaredance directory
for filename in os.listdir(squaredance_dir):
    if filename.endswith(".csv"):
        # Extract start_date and end_date from the filename
        parts = filename.replace('.csv', '').split('_')
        
        # Parse start_date and end_date from parts
        start_date = datetime.strptime(parts[0], "%b%d").replace(year=2024)
        end_date = datetime.strptime(parts[1], "%b%d").replace(year=2024)
        
        # Determine the week number based on start_date (this is optional and can be customized)
        week = start_date.isocalendar().week

        # Load the CSV file into a DataFrame
        filepath = os.path.join(squaredance_dir, filename)
        df = pd.read_csv(filepath)
        
        # Add the new columns to the DataFrame
        df['week'] = week
        df['start_date'] = start_date.strftime("%Y-%m-%d")
        df['end_date'] = end_date.strftime("%Y-%m-%d")
        
        # Relabel the columns
        df.rename(columns={
            'money': 'spend'
        }, inplace=True)
        
        # Remove the affiliate ID from the affiliate_name column
        df['affiliate_name'] = df['affiliate_name'].str.replace(r"\s*\(\d+\)", "", regex=True)
        
        # Keep only the required columns
        df = df[['week', 'start_date', 'end_date', 'affiliate_id', 'affiliate_name', 'spend']]
        
        # Append the processed DataFrame to the combined DataFrame
        combined_df = pd.concat([combined_df, df], ignore_index=True)

# Sort the combined df by week number
combined_df = combined_df.sort_values(by='week')

# Remove rows where spend = 0
combined_df = combined_df[combined_df['spend'] != 0]

# Save the combined DataFrame to a new table in SQLite
combined_df.to_sql('bonafide_sqd_compiled', conn, if_exists='replace', index=False)

# Print the combined DataFrame to verify
pd.set_option('display.max_rows', None)
print(combined_df)

# Close the database connection
conn.close()

