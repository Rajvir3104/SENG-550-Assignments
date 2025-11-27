import pandas as pd
import os

df = pd.read_csv("orders.csv")

for dow in range(7):
    subset = df[df["order_dow"] == dow]
    
    outdir = f"part1/data/raw/{dow}"
    os.makedirs(outdir, exist_ok=True)
    
    subset.to_csv(f"{outdir}/orders_{dow}.csv", index=False)