import pandas as pd
import numpy as np

# Define the weeks and marketing channels
weeks = [1, 2, 3, 4]
channels = ["Google", "Meta", "TikTok", "Affiliate", "Display", "Native", "Pinterest"]

# Initialize lists to hold the data
data = []

# Generate sample data
np.random.seed(42)  # For reproducibility
for week in weeks:
    for channel in channels:
        wow_revenue_change = round(np.random.uniform(-10, 20))  # Random WoW Revenue Change between -10% and 20%
        wow_ncac_change = round(np.random.uniform(-5, 15))  # Random WoW nCAC Change between -5% and 15%
        spend = round(np.random.uniform(10000, 50000))  # Random Spend between $10,000 and $50,000
        total_customers_acquired = np.random.randint(100, 1000)  # Random number of customers acquired
        new_customers_acquired = np.random.randint(50, total_customers_acquired)  # Random number of new customers acquired
        pct_new_customers_acquired = round(new_customers_acquired / total_customers_acquired * 100)  # Percentage of new customers
        aov_all_customers = round(np.random.uniform(50, 200))  # Random AOV for all customers between $50 and $200
        aov_new_customers = round(np.random.uniform(50, 200))  # Random AOV for new customers between $50 and $200
        revenue_new_customers = round(new_customers_acquired * aov_new_customers)  # Revenue from new customers
        revenue_new_returning_customers = round(total_customers_acquired * aov_all_customers)  # Revenue from new & returning customers
        ncac = round(spend / new_customers_acquired)  # New Customer Acquisition Cost
        cac = round(spend / total_customers_acquired)  # Customer Acquisition Cost

        # Append the data
        data.append([
            week,
            channel,
            wow_revenue_change,
            wow_ncac_change,
            spend,
            total_customers_acquired,
            new_customers_acquired,
            pct_new_customers_acquired,
            aov_all_customers,
            aov_new_customers,
            revenue_new_customers,
            revenue_new_returning_customers,
            ncac,
            cac
        ])

# Create a DataFrame
columns = [
    "Week", "Marketing Channel", "WoW Revenue Change", "WoW nCAC Change", "Spend", 
    "Number of All Customers Acquired", "Out of All Customers Acquired, Number of New Customers Acquired", 
    "% of New Customers Acquired", "AOV for All Customers", "AOV for New Customers Only", 
    "Revenue from New Customers Only", "Revenue from New and Returning Customers Acquired", 
    "nCAC", "CAC"
]
df = pd.DataFrame(data, columns=columns)

# Display the DataFrame (optional)
print(df)

# Save to CSV if needed
df.to_csv("/Users/chandnimelwani/Documents/MMAI/Capstone/code_repo/capstone-mas/data/generated_marketing_data.csv", index=False)
