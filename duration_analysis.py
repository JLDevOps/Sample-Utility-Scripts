import pymssql
import pandas as pd
from datetime import datetime
import pytz

# --- Config ---
server = 'YOUR_SERVER'
user = 'YOUR_USER'
password = 'YOUR_PASSWORD'
database = 'YOUR_DATABASE'
output_csv = 'duration_analysis_with_rto.csv'

mapping = {
    0: (1, 2),  
    1: (4, 6),   
    2: (8, 10),
}

# --- Connect to SQL Server ---
conn = pymssql.connect(server=server, user=user, password=password, database=database)
query = "SELECT app_id, rto, start_time_utc, end_time_utc FROM your_table"
df = pd.read_sql(query, conn)
conn.close()

ct_tz = pytz.timezone('US/Central')
utc_tz = pytz.utc

fixed_start_ct = ct_tz.localize(datetime(2025, 3, 21, 20, 0)) 
fixed_start_utc = fixed_start_ct.astimezone(utc_tz)
print("Fixed Start Time (UTC):", fixed_start_utc)

def compute_duration(row):
    end_time = row['end_time_utc'].replace(tzinfo=utc_tz)
    duration = end_time - fixed_start_utc
    return duration.total_seconds() / 3600

def check_rto(row):
    rto = row['rto']
    duration = row['duration_hours']
    
    if rto not in mapping:
        return 0 
    
    low, high = mapping[rto]
    
    if low <= duration <= high:
        return 1 
    else:
        return 0

df['duration_hours'] = df.apply(compute_duration, axis=1)
df['MET_RTO'] = df.apply(check_rto, axis=1)

# --- Export ---
df.to_csv(output_csv, index=False)

print(f"Result in {output_csv}")


WITH cte AS (
    SELECT 
        app_id,
        rto,
        start_time_utc,
        end_time_utc,
        -- Convert '2025-03-21 20:00:00' CT to UTC
        DATEADD(hour, DATEDIFF(hour, GETDATE(), GETUTCDATE()), '2025-03-21 20:00:00') AS fixed_start_utc
    FROM your_table
)
SELECT 
    app_id,
    rto,
    start_time_utc,
    end_time_utc,
    -- Calculate duration in hours
    DATEDIFF(SECOND, cte.fixed_start_utc, cte.end_time_utc) / 3600.0 AS duration_hours,

    -- Determine MET_RTO
    CASE 
        WHEN rto = 0 AND DATEDIFF(SECOND, cte.fixed_start_utc, cte.end_time_utc) / 3600.0 BETWEEN 1 AND 2 THEN 1
        WHEN rto = 1 AND DATEDIFF(SECOND, cte.fixed_start_utc, cte.end_time_utc) / 3600.0 BETWEEN 4 AND 6 THEN 1
        WHEN rto = 2 AND DATEDIFF(SECOND, cte.fixed_start_utc, cte.end_time_utc) / 3600.0 BETWEEN 8 AND 10 THEN 1
        ELSE 0
    END AS MET_RTO
FROM cte;

-- Example: rto_table(rto, min_hours, max_hours)

WITH cte AS (
    SELECT 
        app_id,
        yt.rto,
        start_time_utc,
        end_time_utc,
        DATEADD(hour, DATEDIFF(hour, GETDATE(), GETUTCDATE()), '2025-03-21 20:00:00') AS fixed_start_utc
    FROM your_table yt
)
SELECT 
    cte.app_id,
    cte.rto,
    cte.start_time_utc,
    cte.end_time_utc,
    DATEDIFF(SECOND, cte.fixed_start_utc, cte.end_time_utc) / 3600.0 AS duration_hours,
    CASE WHEN DATEDIFF(SECOND, cte.fixed_start_utc, cte.end_time_utc) / 3600.0 BETWEEN rt.min_hours AND rt.max_hours THEN 1 ELSE 0 END AS MET_RTO
FROM cte
LEFT JOIN rto_table rt
    ON cte.rto = rt.rto;
