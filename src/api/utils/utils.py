import pandas as pd


def dataframe_to_llm_input(df: pd.DataFrame, sample_size: int = 3):
    result = []

    for column in df.columns:
        examples = df[column].dropna().astype(str).unique()[:sample_size].tolist()

        result.append({"column": column, "examples": examples})

    return result
