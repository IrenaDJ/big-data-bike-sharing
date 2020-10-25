import pandas as pd

base = pd.read_parquet("G:\\Master\\Big Data\\Data\\D.C\\New\\HELP\\empty_base.parquet")
generated = pd.read_parquet("G:\\Master\\Big Data\\Data\\D.C\\New\\ml_dataset_extended_final.parquet")

reduced = generated[["hour", "dayofyear", "month", "air_temp", "visibility", "wind_speed", "weather_class", "start_station_id", "count"]]
reduced.columns = ["hour", "dayofyear", "month", "air_temp", "visibility", "wind_speed", "weather_class", "station", "count"]

result = pd.concat([reduced, base])
result = result.sort_values(by=['count'], ascending=False)
result = result.drop_duplicates(subset=["hour", "dayofyear", "month", "station"], keep='first')
result.to_parquet("G:\\Master\\Big Data\\Data\\D.C\\New\\HELP\\final.parquet")