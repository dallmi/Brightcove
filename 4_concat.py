import pandas as pd
from datetime import datetime

# === UPDATE Time range ===
date_fmt = "%Y_%m_%d"
to_date = datetime.now().strftime(date_fmt)

# Input files dictionary
input_files = {
    "internet": {
        "2024": "2024/daily_analytics_summary_2023_01_01_to_2024_12_31.csv",
        "2025": f"2025/daily_analytics_summary_2025_01_01_to_{to_date}.csv"
    },
    "research": {
        "2024": "2024/daily_analytics_summary_research_2023_01_01_to_2024_12_31.csv",
        "2025": f"2025/daily_analytics_summary_research_2025_01_01_to_{to_date}.csv"
    }
}

# Loop through the dictionary and concatenate files
for category, files in input_files.items():
    dfs = []
    for year, file_path in files.items():
        df = pd.read_csv(file_path)
        dfs.append(df)
    combined_df = pd.concat(dfs, ignore_index=True)
    output_file = f"Q:/Brightcove/Reporting/daily_analytics_2023_2024_2025_{category}.csv"
    combined_df.to_csv(output_file, index=False)
    print(f"Combined data written to {output_file}")