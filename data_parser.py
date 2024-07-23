import argparse
import requests
import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from shapely.geometry import Point
import json
if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Data Processor')
  parser.add_argument('--input_file', type=str, required=True, help='Path to the input file')
  parser.add_argument('--geometry_file', type=str, required=True, help='Path to the geometry file')
  parser.add_argument('--output_file', type=str, required=True, help='Path to the output file')
  parser.add_argument('--group_by_day', action='store_true', help='Flag to group data by day for daily calculations')
  # Name: ID timestamp latitude longtitude
  parser.add_argument('--selected_columns', nargs='+', required=True, help='List of columns to select')
  parser.add_argument('--geometry_columns', nargs='+', required=True, help='List of columns to select')
  parser.add_argument('--start_date', type=str, default='1900-01-01', help='Start date for filtering data (inclusive)')
  parser.add_argument('--end_date', type=str, default='2100-01-01', help='End date for filtering data (inclusive)')
  args = parser.parse_args()

  id_name = args.selected_columns[0]
  Time_name = args.selected_columns[1]
  Latitude_name = args.selected_columns[2]
  Longitude_name = args.selected_columns[3]

  gid_name = args.geometry_columns[0]
  g_name = 'geometry'
  # 将CSV数据转换为Pandas DataFrame
  data = pd.read_csv(args.input_file, on_bad_lines='skip')
  df = pd.DataFrame(data)
  # 筛选
  df_s = df[args.selected_columns]
  df_s.dropna(inplace=True)
  # df_s[Latitude_name] = pd.to_numeric(df_s[Latitude_name], errors='coerce')
  # df_s[Longitude_name] = pd.to_numeric(df_s[Longitude_name], errors='coerce')
  df_s.dropna(inplace=True)

  
  # 重置索引
  df_s.reset_index(drop=True, inplace=True)
  df_s[Time_name] = pd.to_datetime(df_s[Time_name], errors='coerce')
  df_s = df_s[(df_s[Time_name] >= args.start_date) & (df_s[Time_name] <= args.end_date)]
  
  gdf = gpd.read_file(args.geometry_file)
  ## 圈出曼哈顿
  gdf= gdf[gdf['borough'] == 'Manhattan']
  polygon_count = {idx: 0 for idx in gdf[gid_name]}
  geometries = gdf[[gid_name, g_name,]]
  if(args.group_by_day):
    df_s['group_date'] = df_s[Time_name].dt.date
    grouped = df_s.groupby('group_date')
    daily_polygon_count = {}
    for date, group in grouped:
        polygon_count = {idx: 0 for idx in gdf[gid_name]}
        for idx, row in group.iterrows():
            target_point = Point(row[Longitude_name], row[Latitude_name])
            contained_polygons = gdf[gdf.contains(target_point)]
            if not contained_polygons.empty:
                for poly_id in contained_polygons[gid_name]:
                    polygon_count[poly_id] += 1
        daily_polygon_count[str(date)] = polygon_count

    output_json = json.dumps(daily_polygon_count, ensure_ascii=False, indent=4)
  else:
    for idx, row in df_s.iterrows():
        target_point = Point(row[Longitude_name], row[Latitude_name])
        contained_polygons = gdf[gdf.contains(target_point)]
        if not contained_polygons.empty:
            for poly_id in contained_polygons[gid_name]:
              polygon_count[poly_id] += 1
    output_json = json.dumps(polygon_count, ensure_ascii=False, indent=4)
  with open(args.output_file, 'w', encoding='utf-8') as f:
    f.write(output_json)
  print(df_s)
