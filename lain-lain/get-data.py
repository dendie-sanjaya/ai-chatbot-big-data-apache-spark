# Membaca dari CSV
df_csv = spark.read.csv("path/to/your/file.csv", header=True, inferSchema=True)

# Membaca dari Parquet
df_parquet = spark.read.parquet("path/to/your/data.parquet")

# Membaca dari JDBC Database (misal PostgreSQL)
jdbc_url = "jdbc:postgresql://localhost:5432/mydatabase"
df_db = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "public.my_table") \
    .option("user", "myuser") \
    .option("password", "mypassword") \
    .load()