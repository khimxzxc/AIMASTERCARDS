import pandas as pd

INPUT_FILE = "DECENTRATHON_3.0.parquet"
OUTPUT_FILE = "client_features.xlsx"

print("[INFO] Чтение данных...")
df = pd.read_parquet(INPUT_FILE)

print("[INFO] Агрегация признаков...")
df["is_wallet"] = df["wallet_type"].notna()
df["is_salary"] = df["transaction_type"] == "SALARY"

client_features = df.groupby("card_id").agg(
    total_txns=("transaction_amount_kzt", "count"),
    avg_txn_amt=("transaction_amount_kzt", "mean"),
    pct_food=("merchant_mcc", lambda x: sum(mcc in [5411, 5812, 5814] for mcc in x.dropna()) / len(x)),
    pct_travel=("merchant_mcc", lambda x: sum(mcc in [4111, 4511, 7011, 7012] for mcc in x.dropna()) / len(x)),
    pct_wallet_use=("is_wallet", "mean"),
    salary_flag=("is_salary", "any"),
    unique_cities=("merchant_city", lambda x: x.nunique())
).reset_index()

print(f"[✅] Сохраняем: {OUTPUT_FILE}")
client_features.to_excel(OUTPUT_FILE, index=False)
