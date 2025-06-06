from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# --- 1. Inisialisasi SparkSession ---
# SparkSession adalah entry point untuk memprogram Spark dengan DataFrame dan SQL.
# .master("local[*]") berarti Spark akan berjalan secara lokal menggunakan semua core CPU yang tersedia.
# .appName("ContohSparkSQL") adalah nama aplikasi Spark Anda.
spark = SparkSession.builder \
    .appName("ContohSparkSQL") \
    .master("local[*]") \
    .getOrCreate()

print("SparkSession berhasil diinisialisasi.")
print(f"Versi Spark: {spark.version}")
print("-" * 30)

# --- 2. Membuat Data (Contoh dalam Memori) ---
# Kita akan membuat data sederhana tentang beberapa produk.
data = [
    ("Laptop", "Elektronik", 1200, 50),
    ("Mouse", "Elektronik", 25, 200),
    ("Meja", "Furniture", 150, 30),
    ("Kursi Gaming", "Furniture", 300, 15),
    ("Keyboard", "Elektronik", 75, 100),
    ("Buku Python", "Edukasi", 30, 80),
    ("Buku Go", "Edukasi", 40, 60)
]

# Mendefinisikan Skema (schema) untuk data kita
schema = StructType([
    StructField("nama_produk", StringType(), True),
    StructField("kategori", StringType(), True),
    StructField("harga", IntegerType(), True),
    StructField("stok", IntegerType(), True)
])

# Membuat DataFrame dari data dan skema
df_produk = spark.createDataFrame(data, schema)

print("DataFrame 'df_produk' berhasil dibuat:")
df_produk.printSchema() # Menampilkan skema DataFrame
df_produk.show()       # Menampilkan beberapa baris data dalam DataFrame
print("-" * 30)

# --- 3. Mendaftarkan DataFrame sebagai Temporary View ---
# Ini memungkinkan kita untuk mengkueri DataFrame menggunakan SQL.
df_produk.createOrReplaceTempView("produk_inventori")

print("DataFrame 'df_produk' telah didaftarkan sebagai temporary view 'produk_inventori'.")
print("-" * 30)

# --- 4. Menjalankan Kueri SQL Menggunakan Spark SQL ---
print("Hasil kueri SQL: Produk dengan harga > 100 dan kategori 'Elektronik'")
sql_result = spark.sql("""
    SELECT nama_produk, harga, stok
    FROM produk_inventori
    WHERE harga > 100 AND kategori = 'Elektronik'
    ORDER BY harga DESC
""")
sql_result.show()
print("-" * 30)

print("Hasil kueri SQL: Total Stok per Kategori")
sql_total_stok = spark.sql("""
    SELECT kategori, SUM(stok) AS total_stok
    FROM produk_inventori
    GROUP BY kategori
    ORDER BY total_stok DESC
""")
sql_total_stok.show()
print("-" * 30)

# --- 5. Melakukan Operasi DataFrame API (Alternatif untuk SQL) ---
print("Hasil operasi DataFrame API: Filter kategori 'Furniture' dan pilih 'nama_produk', 'harga'")
df_furniture = df_produk.filter(df_produk["kategori"] == "Furniture") \
                        .select("nama_produk", "harga")
df_furniture.show()
print("-" * 30)

print("Hasil operasi DataFrame API: Agregasi Rata-rata Harga per Kategori")
from pyspark.sql import functions as F
avg_harga_per_kategori = df_produk.groupBy("kategori") \
                                  .agg(F.avg("harga").alias("rata_rata_harga")) \
                                  .orderBy(F.col("rata_rata_harga").desc())
avg_harga_per_kategori.show()
print("-" * 30)

# --- 6. Hentikan SparkSession ---
# Penting untuk selalu menghentikan SparkSession setelah selesai agar sumber daya dilepaskan.
spark.stop()
print("SparkSession telah dihentikan.")