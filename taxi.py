import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sodapy import Socrata
import utils

class TaxiDataProcessor:
    def __init__(self, year, interval_minutes, selected_columns, total_records, client=None):
        self.year = year
        self.interval_minutes = interval_minutes
        self.selected_columns = selected_columns
        self.total_records = total_records
        self.client = client if client else Socrata("data.cityofchicago.org", None)
        self.vector3D = np.zeros((77, 77, 20000))
        self.time_slot_mapping = self.get_time_slot_mapping(year)
        self.slot_numbers = set()
        self.slot_counts = {}

    def get_time_slot_mapping(self, year):
        is_leap_year = (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)
        days_in_year = 366 if is_leap_year else 365

        time_slots = {}
        start_time = datetime(year, 1, 1)
        slot_number = 0

        for day in range(days_in_year):
            for interval in range(0, 1440, self.interval_minutes):
                current_time = start_time + timedelta(days=day, minutes=interval)
                time_slots[current_time] = slot_number
                slot_number += 1

        return time_slots

    def map_datetime_to_slot(self, dt):
        closest_time = min(self.time_slot_mapping.keys(), key=lambda k: abs(k - dt))
        return self.time_slot_mapping[closest_time]

    def process_data(self):
        df_list = []

        for offset in range(0, self.total_records, 1000):
            print(f"Fetching records {offset} to {offset + 999}...")
            data = utils.request_data("data.cityofchicago.org", 1000, offset)
            results_df = pd.DataFrame.from_records(data)
            df_list.append(results_df)

        results_df = pd.concat(df_list, ignore_index=True)
        results_df = results_df[self.selected_columns]
        results_df.dropna(inplace=True)
        results_df['trip_start_timestamp'] = pd.to_datetime(results_df['trip_start_timestamp'], errors='coerce')

        for idx, row in results_df.iterrows():
            slot_number = self.map_datetime_to_slot(row['trip_start_timestamp'])
            self.slot_numbers.add(slot_number)
            if slot_number in self.slot_counts:
                self.slot_counts[slot_number] += 1
            else:
                self.slot_counts[slot_number] = 1
            self.vector3D[int(row['pickup_community_area']) - 1, int(row['dropoff_community_area']) - 1, slot_number] += 1

    def print_results(self):
        print("vector3D")
        print(self.vector3D)

        print("收集到的 slot_number:")
        print(self.slot_numbers)

        print("slot_number 的出现次数:")
        for slot_number, count in self.slot_counts.items():
            print(f"Slot number {slot_number}: {count} 次")

        for slot_number in self.slot_numbers:
            slice_2D = self.vector3D[:, :, slot_number]
            print(f"Slot number {slot_number} 的二维切片:")
            print(slice_2D)
            print(slice_2D.shape)  # 输出应该是 (77, 77)
    def save_vector3d(self, filename):
        np.save(filename, self.vector3D)
        print(f"vector3D saved to {filename}")

if __name__ == "__main__":
    selected_columns = ['trip_id', 'taxi_id', 'trip_start_timestamp', 'trip_end_timestamp', 'pickup_community_area', 'dropoff_community_area']
    total_records = 10000
    interval_minutes = 30  # 可以替换为60, 120, 240, 360, 720, 1440等

    processor = TaxiDataProcessor(2024, interval_minutes, selected_columns, total_records)
    processor.process_data()
    processor.print_results()
    processor.save_vector3d('res.npy')
