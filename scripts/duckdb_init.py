import duckdb


def load_demo_data():
    with duckdb.connect("data/file.db") as con:
        con.sql("CREATE OR REPLACE TABLE customers (business_name STRING, web_site STRING)")
        con.sql("INSERT INTO customers SELECT business_name, domain FROM 'examples/example_file.csv'")
        con.table("customers").show()


if __name__ == "__main__":
    load_demo_data()
