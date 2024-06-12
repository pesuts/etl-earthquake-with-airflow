## Documentasi Proyek Data Engineer: ETL Dataset Gempa Bumi

### 1. Pendahuluan

Proyek ini bertujuan untuk melakukan proses ETL (Extract, Transform, Load) pada dataset gempa bumi. Data gempa bumi akan diintegrasikan dengan data demografi, geologi, infrastruktur, dan historis. Proses ETL ini dilakukan dengan menggunakan beberapa alat yaitu Pentaho, Apache Spark, Google Cloud Storage (GCS), dan Google BigQuery, serta diatur dengan Apache Airflow.

### 2. Arsitektur Proyek

#### a. Alat dan Teknologi yang Digunakan
- **Pentaho**: Untuk mengekstrak data dari file CSV dan memasukkan ke dalam database PostgreSQL.
- **Apache Spark**: Untuk transformasi data dan menyimpan hasil transformasi dalam format star schema.
- **Google Cloud Storage (GCS)**: Untuk penyimpanan sementara data hasil transformasi.
- **Google BigQuery**: Untuk penyimpanan akhir data yang sudah diproses.
- **Apache Airflow**: Untuk mengatur dan mengotomatisasi proses ETL.

### 3. Proses ETL

#### a. Extract
Proses ekstraksi dilakukan dengan Pentaho, di mana data CSV diambil dan dimasukkan ke dalam database PostgreSQL. File CSV diolah menjadi tabel-tabel berikut:
- `earthquake`
- `demographic`
- `geological`
- `infrastructure`
- `historical`

#### b. Transform
Transformasi data dilakukan menggunakan Apache Spark. Berikut adalah langkah-langkah yang dilakukan dalam proses transformasi:
1. Membuat Spark Session dengan konfigurasi untuk koneksi ke GCS dan PostgreSQL.
2. Mengambil data dari PostgreSQL dan memuatnya ke dalam DataFrame Spark.
3. Melakukan berbagai transformasi pada tabel `earthquake`:
   - Memisahkan kolom `place` menjadi `region` dan `detail`.
   - Mengubah tipe data `latitude` dan `longitude` menjadi `DoubleType`.
   - Menghitung median untuk beberapa kolom dan mengisi nilai kosong dengan median tersebut.
4. Menggabungkan tabel `earthquake` dengan tabel-tabel lain berdasarkan kolom `latitude` dan `longitude`.
5. Membuat tabel dimensi (`dim_location`, `dim_time`, `dim_demographic`, `dim_infrastructure`, `dim_geological`, `dim_historical`) dan tabel fakta (`fact_earthquake`).
6. Menyimpan data hasil transformasi ke GCS dalam format Parquet.

#### c. Load
Proses loading dilakukan dengan Apache Airflow, di mana data yang sudah disimpan dalam GCS akan dimuat ke Google BigQuery.

#### 4. Screenshots

![](Screenshot%20(2431).png)
![](Screenshot%20(2432).png)
![](Screenshot%20(2435).png)
![](Screenshot%20(2436).png)