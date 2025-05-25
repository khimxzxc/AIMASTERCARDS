import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

INPUT_FILE = "client_features.xlsx"
OUTPUT_FILE = "client_segments.parquet"

print("[INFO] Загрузка признаков...")
df = pd.read_excel(INPUT_FILE)

features = df.drop(columns=["card_id"])
features = pd.get_dummies(features, columns=["salary_flag"], drop_first=True)

scaler = StandardScaler()
X_scaled = scaler.fit_transform(features)

print("[INFO] Обучаем KMeans...")
kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
df["segment_id"] = kmeans.fit_predict(X_scaled)

print(f"[✅] Сохраняем сегменты: {OUTPUT_FILE}")
df[["card_id", "segment_id"]].to_parquet(OUTPUT_FILE, index=False)
