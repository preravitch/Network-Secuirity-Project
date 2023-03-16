# Synthetic log data
import random
import pandas as pd

events = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']

data = {
    'timestamp': pd.date_range('2021-01-01', '2021-01-10', freq='H'),
    'ip': [f"192.168.0.{random.randint(1, 10)}" for _ in range(240)],
    'event': [random.choice(events) for _ in range(240)],
    'details': [f"Details {random.randint(1, 100)}" for _ in range(240)]
}

logs_df = pd.DataFrame(data)
logs_df.to_csv("logs.csv", index=False)

# Read the synthetic logs using Spark
logs_df = spark.read.csv("logs.csv", header=True, inferSchema=True)

#example log
#timestamp,ip,event,details
#2021-01-01 00:00:00,192.168.0.2,A,Details 23
#2021-01-01 01:00:00,192.168.0.4,B,Details 54
#2021-01-01 02:00:00,192.168.0.8,C,Details 12
#2021-01-01 03:00:00,192.168.0.1,D,Details 67
#2021-01-01 04:00:00,192.168.0.3,A,Details 89
#2021-01-01 05:00:00,192.168.0.6,B,Details 45
#2021-01-01 06:00:00,192.168.0.7,C,Details 76
#2021-01-01 07:00:00,192.168.0.5,D,Details 98
#2021-01-01 08:00:00,192.168.0.2,A,Details 34
#2021-01-01 09:00:00,192.168.0.4,C,Details 56