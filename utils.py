import pandas as pd
from sodapy import Socrata
from datetime import datetime, timedelta

def request_data(url,limit,offset):
  client = Socrata(url, None)
  data = client.get("ajtu-isnz", limit=limit,offset=offset)
  results_df = pd.DataFrame.from_records(data)
  return results_df

def get_time_slot_mapping(year):
    # 检查是否为闰年
    is_leap_year = (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)
    days_in_year = 366 if is_leap_year else 365

    # 创建时间段映射
    time_slots = {}
    start_time = datetime(year, 1, 1)
    slot_number = 0

    for day in range(days_in_year):
        for half_hour in range(48):
            current_time = start_time + timedelta(days=day, minutes=30*half_hour)
            time_slots[current_time] = slot_number
            slot_number += 1

    return time_slots


    