import psycopg2

# Define the connection string
connection_string = "postgresql://yash:_fCgzUkbIYTxpvGcZwXExQ@testingone-8980.8nk.gcp-asia-southeast1.cockroachlabs.cloud:26257/defaultdb?sslmode=verify-full"

# Connect to the CockroachDB cluster
try:
    conn = psycopg2.connect(connection_string)
    print("Connected to CockroachDB successfully!")
except Exception as e:
    print(f"Error connecting to CockroachDB: {e}")

# Example: Execute a SQL query
try:
    # Create a cursor
    cur = conn.cursor()

    # Execute a SQL query
    cur.execute("SELECT * FROM your_table;")
    
    # Fetch and print the results
    rows = cur.fetchall()
    for row in rows:
        print(row)

    # Close the cursor
    cur.close()
except Exception as e:
    print(f"Error executing SQL query: {e}")

# Close the connection
conn.close()
