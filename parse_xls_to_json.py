import pandas as pd
import re
from SPARQLWrapper import SPARQLWrapper, JSON
from wikidataintegrator import wdi_core, wdi_login
import json

# Load excel file
df = pd.read_excel(
    "POP2021_20221212.xls", sheet_name="Municípios", skiprows=1, dtype=str
)
df = df.dropna()
# Create a new column by combining 'COD. UF' and 'COD. MUNIC'
df["combined_code"] = df["COD. UF"].astype(str) + df["COD. MUNIC"]

# Clean 'POPULAÇÃO ESTIMADA' and convert it to int
df["POPULAÇÃO ESTIMADA"] = df["POPULAÇÃO ESTIMADA"].apply(
    lambda x: int(re.sub(r"\([^()]*\)", "", x).replace(".", ""))
)

# Create a dict with 'combined_code' and 'POPULAÇÃO ESTIMADA'
population_dict = df.set_index("combined_code")["POPULAÇÃO ESTIMADA"].to_dict()

# Convert population_dict to JSON format
population_json = json.dumps(population_dict, indent=3)

# Write JSON to a file
with open("population.json", "w") as file:
    file.write(population_json)
