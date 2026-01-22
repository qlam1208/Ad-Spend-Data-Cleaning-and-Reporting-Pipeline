import pandas as pd


#xử lý data 1 - 8
df = pd.read_csv(
    "data/data_oct_1_8.csv",
    dtype = {"id_acc": str},
    engine = "python",
    encoding = "utf-8-sig"
)
df.columns = df.columns.str.strip().str.replace("\ufeff", "", regex=False)
#df = df.set_index("id_acc")

#chuẩn hóa spend
df["spend"] = df["spend"].astype('string').str.replace('.','').str.replace(',', '.').astype(float)
#target_id = "747168356871672"

#total_spend = df[df["id_acc"] == target_id]["spend"].sum()

#print(total_spend)

#print(df.loc["8913961160313789"])
##df.groupby("id_acc")["spend"].sum()
#print(df.groupby("id_acc")["spend"].sum())

#xử lý data 9 - 16
#print(df)
df2 = pd.read_csv(
    "data/data_oct_9_16.csv",
    dtype = {"id_acc": str},
    engine = "python",
    encoding= "utf-8-sig"
)
df2.columns = df2.columns.str.strip().str.replace("\ufeff", "", regex=False)
#df = df.set_index("id_acc")

#chuẩn hóa spend
df2["spend"] = df2["spend"].astype('string').str.replace('.','').str.replace(',', '.').astype(float)

df3 = pd.read_csv(
    "data/data_oct_17_24.csv",
    dtype = {"id_acc": str},
    engine = "python",
    encoding= "utf-8-sig"
)
df3.columns = df3.columns.str.strip().str.replace("\ufeff", "", regex=False)
#df = df.set_index("id_acc")

#chuẩn hóa spend
df3["spend"] = df3["spend"].astype('string').str.replace('.','').str.replace(',', '.').astype(float)


df4 = pd.read_csv(
    "data/data_oct_25_31.csv",
    dtype = {"id_acc": str},
    engine = "python",
    encoding= "utf-8-sig"
)
df4.columns = df4.columns.str.strip().str.replace("\ufeff", "", regex=False)
#df = df.set_index("id_acc")

#chuẩn hóa spend
df4["spend"] = df4["spend"].astype('string').str.replace('.','').str.replace(',', '.').astype(float)

df_full = pd.concat(
    [df, df2, df3, df4]
)
#xóa tiêu đề trùng
df_full = df_full[
    (df_full["id_acc"] != "id_acc") &
    (df_full["day"]    != "day") &
    (df_full["spend"]  != "spend")
]
#clean dòng trống
df_full = df_full[
    (df_full["id_acc"].notna()) &
    (df_full["id_acc"] != "") &
    (df_full["id_acc"] != "id_acc")
]
#df_full["id_acc"] = "'" + df_full["id_acc"].astype(str)

#reset index

# df_full.to_csv(
#    "output/df_full_clean.csv",
#    index=False,
#    encoding="utf-8-sig"
# )
#xóa id acc trùng, giữ lại id cuối
df_full = df_full.drop_duplicates(
    subset = ["id_acc", "day"],
    keep = "last"
)

def build_master(df_long: pd.DataFrame) -> pd.DataFrame:
    df = df_long.copy()

    #ép day về int
    df["day"] = pd.to_numeric(df["day"], errors="coerce")
    df = df.dropna(subset=["day"])
    df["day"] = df["day"].astype(int)

    matrix = (
        df.pivot_table(
            index="id_acc",
            columns="day",
            values="spend",
            aggfunc="sum"
        )
        .fillna(0)
    )

    #reindex cho đủ ngày
    matrix = matrix.reindex(columns=list(range(1, 32)), fill_value=0)

    matrix["total_spend"] = matrix.sum(axis=1)

    #day từ int sang string
    matrix.columns = [str(c) for c in matrix.columns[:-1]] + ["total_spend"]

    out = matrix.reset_index()
    out = out[["id_acc", "total_spend"] + [str(d) for d in range(1, 32)]]
    return out

df_test = build_master(df_full)
#df_test.to_csv("output/master_2025-10.csv", index=False, encoding="utf-8-sig")

#mapping clients
mapping = pd.read_csv("data/client_name.csv", dtype={"id_acc": str, "client_name": str})
mapping["id_acc"] = mapping["id_acc"].astype(str).str.strip()
mapping["client_name"] = mapping["client_name"].astype(str).str.strip()

df_test["id_acc"] = df_test["id_acc"].astype(str).str.strip()

df_test_mapped = df_test.merge(mapping, on="id_acc", how="left")

#đọc list khách cần tạo sheet
clients_ctl = pd.read_csv("data/clients_to_export.csv")
clients_ctl["client_name"] = clients_ctl["client_name"].astype(str).str.strip()

clients_need = set(clients_ctl["client_name"])

#tạo file
for client, g in df_test_mapped.groupby("client_name"):
    if client not in clients_need:
        continue

    out = g[["id_acc", "total_spend"] + [str(d) for d in range(1, 32)]]

    filename = f"output/clients/{client}_2025-10.csv"
    out.to_csv(filename, index=False, encoding="utf-8-sig")

    print("Exported:", filename)