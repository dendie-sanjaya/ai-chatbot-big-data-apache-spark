# Menulis ke Parquet
sql_result.write.mode("overwrite").parquet("path/to/output.parquet")

# Menulis ke JDBC Database
sql_total_stok.write.mode("overwrite").jdbc(jdbc_url, "public.total_stok_per_kategori", properties={"user": "myuser", "password": "mypassword"})