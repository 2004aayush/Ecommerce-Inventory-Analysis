import pandas as pd
import sqlite3
df = pd.read_csv("inventry_data.csv")

# remove unwanted column
df = df.drop(columns=["index"], errors="ignore")

# clean column names
df.columns = df.columns.str.strip().str.replace(" ", "_")

# clean text columns
df["Category"] = df["Category"].str.split(":").str[-1].str.strip().str.lower()
df["Color"] = df["Color"].str.strip().str.lower()

# convert stock to numeric
df["Stock"] = pd.to_numeric(df["Stock"], errors="coerce")

#data cleaning
df = df[df["Stock"] != 0]
df = df.dropna()
df = df.drop_duplicates()
conn = sqlite3.connect("ecommerce.db")

# store cleaned data
df.to_sql("products", conn, if_exists="replace", index=False)
df.to_csv("cleaned_data.csv", index=False)


query1 = """
SELECT Category, SUM(Stock) AS total_stock
FROM products
GROUP BY Category
ORDER BY total_stock DESC
LIMIT 5;
"""
cat = pd.read_sql_query(query1, conn)
print("\n CATEGORY-WISE STOCK (Top 5)\n")
print(cat.to_string(index=False))

query2 = """
SELECT Size, SUM(Stock) AS total_stock
FROM products
GROUP BY Size
ORDER BY total_stock DESC;
"""
size = pd.read_sql_query(query2, conn)
print("\n SIZE DISTRIBUTION\n")
print(size.to_string(index=False))

query3 = """
SELECT Color, SUM(Stock) AS total_stock
FROM products
GROUP BY Color
ORDER BY total_stock DESC
LIMIT 5;
"""
color = pd.read_sql_query(query3, conn)
print("\n TOP 5 COLORS\n")
print(color.to_string(index=False))

query4 = """
SELECT SKU_Code, Category, Stock
FROM products
WHERE Stock < 10
ORDER BY Stock ASC
LIMIT 10;
"""
low_stock = pd.read_sql_query(query4, conn)

print("\n LOW STOCK PRODUCTS (Restocking Needed)\n")
print(low_stock.to_string(index=False))

query5 = """
SELECT SKU_Code, Category, Stock
FROM products
ORDER BY Stock DESC
LIMIT 5;
"""
top = pd.read_sql_query(query5, conn)

print("\n TOP 5 PRODUCTS (HIGHEST STOCK)\n")
print(top.to_string(index=False))

print("\n📊 SUMMARY\n")
print(f"Total Products: {len(df)}")
print(f"Total Stock: {int(df['Stock'].sum())}")
conn.close()