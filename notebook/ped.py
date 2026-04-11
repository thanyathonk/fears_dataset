import pandas as pd

# df = pd.read_parquet("/ist/users/thanyathonk/thanyathonk_bak/fears_dataset/data/ped_drugs_llm_cleaned_full_data_updated.parquet")
df = pd.read_parquet("/ist/users/thanyathonk/thanyathonk_bak/fears_dataset/data/staging/s07b_llm_clean/pediatric_drugs_clean_full_data.parquet")

print(df.head())
print("================================================")
print(f"df.shape: {df.shape}")
print("================================================")
print(f"df.columns: {df.columns}")
print("================================================")
print(f"df['ingredient'].nunique(): {df['ingredients'].nunique()}")
print("================================================")
print(f"df['medicinal_product'].nunique(): {df['medicinal_product'].nunique()}")
print("================================================")
print(df.isnull().sum())