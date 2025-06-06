ETL (Entract Transport Load) menggunakan Apache Spark menggunakan Python

Apache Spark adalah sebuah framework pemrosesan data terdistribusi open-source yang sangat populer dan kuat untuk berbagai beban kerja big data, termasuk ETL (Extract, Transform, Load)


Apapache Spartk kita menggunakan bahasa pemrogram python pyspark

Dengan skema seperti dibawah ini 

Pada contoh case di atas membuat program ETL untuk mendapatkan data log activiy dengan format akhir dari Apache Spark 
dengan format Parquet dengan sumber data dari database sql relation (berisikan data device) sedangkan data cvs (beriksa log device)

kedua sumber data  yaitu database sql relation (berisikan data device) dan cvs (beriksa log device) akan di join oleh A
Apache Spart




jadi file terpisah (1 untuk get data dan conver ke parquer format) dan dan query apash spartk untuk studi case untuk melihat log activiy perantat saat online, offlien, standt dengan join key field device id 



Ketika Anda menjalankan perintah pip install pyspark, pip tidak hanya menginstal binding Python untuk Spark. Ia juga mengunduh dan menyertakan binary Apache Spark lengkap (file JAR, script, dll.) yang diperlukan agar Spark dapat berjalan, meskipun dalam mode lokal


sudo apt install openjdk-17-jdk
5.png

6.png


Jadi, meskipun kodenya menggunakan master("local[*]") yang berarti Spark akan berjalan di komputer Anda sendiri (localhost), Anda tidak perlu pergi ke situs web Apache Spark untuk mengunduh dan menginstal paket Spark secara manual. Instalasi PySpark melalui pip sudah mengurus itu untuk Anda.



Untuk skala production apache spart dapat menggunakan yg type cluster




Unduh Driver: Unduh versi terbaru dari SQLite JDBC driver dari repositori Maven Central:

    Cari org.xerial:sqlite-jdbc di Maven Central.
    Pilih versi terbaru yang stabil (misalnya, 3.45.1.0). Klik "jar" untuk mengunduh file .jar tersebut (misalnya, sqlite-jdbc-3.45.1.0.jar).


pip install pyspark