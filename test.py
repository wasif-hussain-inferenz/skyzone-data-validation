import snowflake.connector

conn = snowflake.connector.connect(
    user="DEV_SVCCONNECTION",
    account="pk81200.west-us-2.azure",
    private_key_file="rsa_key.p8",
    private_key_file_pwd="le9beb2mab9"
)

cur = conn.cursor()

# Use the database we found
cur.execute("USE DATABASE GOLD_DB")
cur.execute("USE SCHEMA DW")

# Check latest date in FACTREVENUE
print("=== Latest dates in FACTREVENUE ===")
cur.execute("""
SELECT DISTINCT RECORDDATE 
FROM FACTREVENUE 
ORDER BY RECORDDATE DESC 
LIMIT 5
""")
for row in cur.fetchall():
    print(row[0])

conn.close()