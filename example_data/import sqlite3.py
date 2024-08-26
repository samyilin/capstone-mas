import sqlite3
import pandas as pd
from datetime import datetime, timedelta

# Connect to SQLite database
conn = sqlite3.connect('bonafide_master.db')
cursor = conn.cursor()

# Load the EntityExport CSV file
entity_export_path = "data/data_transformations/bonafide/Affiliate Network Platform/Everflow/EntityExport_2024-07-01_2024-08-11.csv"
entity_export_df = pd.read_csv(entity_export_path)

# Remove rows where affiliate_name is "test partner"
entity_export_df = entity_export_df[entity_export_df['affiliate_name'] != 'test partner']

# Rename 'payout' to 'spend'
entity_export_df.rename(columns={'payout': 'spend'}, inplace=True)

# Create 'week', 'start_date', and 'end_date'
entity_export_df['date'] = pd.to_datetime(entity_export_df['date'])
entity_export_df['week'] = entity_export_df['date'].dt.isocalendar().week

# Calculate start_date and end_date for each week
def calculate_week_dates(date):
    start_of_week = date - timedelta(days=date.weekday())
    end_of_week = start_of_week + timedelta(days=6)
    return start_of_week, end_of_week

entity_export_df['start_date'], entity_export_df['end_date'] = zip(*entity_export_df['date'].apply(calculate_week_dates))

# Keep only the required columns
bonafide_ef_data = entity_export_df[['week', 'start_date', 'end_date', 'network_affiliate_id', 'affiliate_name', 'spend']]

# Load DataFrame into SQLite
bonafide_ef_data.to_sql('bonafide_ef_data', conn, if_exists='replace', index=False)

# Step 1: Update start_date and end_date to remove time component
cursor.execute("""
UPDATE bonafide_ef_data
SET start_date = DATE(start_date),
    end_date = DATE(end_date);
""")
conn.commit()

# Step 2: Create a new temporary table with the data ordered by week
cursor.execute("""
CREATE TABLE bonafide_ef_data_ordered AS
SELECT
    week,
    DATE(start_date) AS start_date,
    DATE(end_date) AS end_date,
    network_affiliate_id as affiliate_id,
    affiliate_name,
    spend
FROM
    bonafide_ef_data
ORDER BY
    week;
""")
conn.commit()

# Step 3: Replace the old bonafide_ef_data with the ordered table
cursor.execute("DROP TABLE bonafide_ef_data;")
cursor.execute("ALTER TABLE bonafide_ef_data_ordered RENAME TO bonafide_ef_data;")
conn.commit()

# Load the data from bonafide_ef_data table
df = pd.read_sql_query("SELECT * FROM bonafide_ef_data", conn)

# Convert start_date and end_date to datetime
df['start_date'] = pd.to_datetime(df['start_date'])
df['end_date'] = pd.to_datetime(df['end_date'])

# Custom week date ranges based on your requirements
def adjust_week_dates(df):
    # Initialize variables
    week_start_date = pd.Timestamp('2024-07-29')
    week_end_date = week_start_date + timedelta(days=3)  # First week ends on August 1
    current_week = 1
    
    week_start_dates = []
    week_end_dates = []
    weeks = []
    
    for i in range(len(df)):
        current_date = df.loc[i, 'start_date']
        
        # Check if current date is within the current week range
        if week_start_date <= current_date <= week_end_date:
            week_start_dates.append(week_start_date)
            week_end_dates.append(week_end_date)
            weeks.append(current_week)
        else:
            # Move to the next week
            current_week += 1
            week_start_date = week_end_date + timedelta(days=1)  # Start the next week
            week_end_date = week_start_date + timedelta(days=6)  # End of the next week
            week_start_dates.append(week_start_date)
            week_end_dates.append(week_end_date)
            weeks.append(current_week)
    
    df['week'] = weeks
    df['start_date'] = pd.Series(week_start_dates).dt.date
    df['end_date'] = pd.Series(week_end_dates).dt.date
    
    return df

# Apply the adjustment
df = adjust_week_dates(df)

# Save the adjusted data back into the SQLite database
df.to_sql('bonafide_ef_data', conn, if_exists='replace', index=False)

# Print the entire DataFrame to see all the weeks, including week 3
print(df)

# Close the connection
conn.close()