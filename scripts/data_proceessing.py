import pandas as pd
import os

base_path = "./market_db/"

def remove_dollar_signs(file_path: str):
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    content = content.replace("$", "")
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)


def remove_duplicate_rows(df: pd.DataFrame) -> None:
    initial_rows = len(df)
    df = df.drop_duplicates(keep="first")
    removed = initial_rows - len(df)
    print(f"Removed {removed} duplicate rows.")
    return df


def transform_sales_pk(df: pd.DataFrame) -> None:
    pk_col = "SalesKey"
    df[pk_col] = range(1, len(df) + 1)
    return df


def process_data():
    for file_name in os.listdir(base_path):
        if file_name.endswith(".csv"):
            file_path = os.path.join(base_path, file_name)
            output_path = os.path.join(base_path, file_name)

            remove_dollar_signs(file_path)
            df = pd.read_csv(
                file_path,
                sep=None,
                engine="python",
                skip_blank_lines=True,
                on_bad_lines="skip",
            )

            df = remove_duplicate_rows(df)

            if file_name == "Sales.csv":
                df = transform_sales_pk(df)

            df.to_csv(output_path, sep=";", index=False)
