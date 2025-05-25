import pandas as pd

# Чтение Excel с признаками
features = pd.read_excel("client_features.xlsx")  # или .csv, если хочешь
segments = pd.read_parquet("client_segments.parquet")

# Объединение
merged = features.merge(segments, on="card_id")

# Сохраняем
merged.to_excel("clients_full.xlsx", index=False)
merged.to_parquet("clients.parquet", index=False)

print(f"✅ Объединено: {len(merged)} клиентов")
