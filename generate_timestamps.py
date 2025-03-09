from datetime import datetime, timedelta
import json

# Define the year
year = 2024

# Initialize list
timestamps = []

# Loop through each month, day, and hour
start_date = datetime(2024, 3, 1, 0)  # March 1, 00:00
end_date = datetime(2025, 2, 28, 23)  # December 31, 23:00

current_date = start_date
while current_date <= end_date:
    timestamps.append(f"{current_date.year}-{current_date.month:02d}-{current_date.day:02d}-{current_date.hour}")  
    current_date += timedelta(hours=1)

# Wrap the array inside an object
timestamps_json = {"timestamps": timestamps}

# Save to JSON file
with open("timestamps.json", "w") as f:
    json.dump(timestamps_json, f, indent=4)

print("JSON file with timestamps generated successfully!")