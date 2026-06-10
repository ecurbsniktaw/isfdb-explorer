import re
import pandas as pd

# Path to your local Nginx log file
LOG_FILE_PATH = "access.log"

# Regular expression matching Nginx "combined" log format
LOG_PATTERN = re.compile(
    r'(?P<ip>\S+)\s+\S+\s+(?P<user>\S+)\s+\[(?P<time>[^\]]+)\]\s+'
    r'"(?P<method>\S+)\s+(?P<request>[^\s"]+)\s*[^"]*"\s+'
    r'(?P<status>\d{3})\s+(?P<size>\d+)\s+'
    r'"(?P<referrer>[^"]*)"\s+"(?P<user_agent>[^"]*)"'
)

def parse_nginx_logs(file_path):
    parsed_lines = []
    
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            match = LOG_PATTERN.match(line)
            if match:
                data = match.groupdict()
                # Convert size column to integer for calculations
                data["size"] = int(data["size"])
                parsed_lines.append(data)
                
    return pd.DataFrame(parsed_lines)

# Load data into Pandas
df = parse_nginx_logs(LOG_FILE_PATH)

# Check if logs exist before analyzing
if not df.empty:
    print("--- Top 5 IP Addresses ---")
    print(df["ip"].value_counts().head(5))
    
    print("\n--- HTTP Status Code Breakdown ---")
    print(df["status"].value_counts())
    
    print("\n--- Top 5 Requested Pages ---")
    print(df["request"].value_counts().head(5))
    
    print(f"\nTotal Data Served: {df['size'].sum() / (1024*1024):.2f} MB")
else:
    print("No log lines parsed successfully. Check your log format pattern.")
