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

### 4. Implementasi Kode

#### a. Kode Spark untuk Transformasi Data

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    split,
    monotonically_increasing_id,
    year,
    month,
    dayofmonth,
    hour,
)
from pyspark.sql.types import DoubleType

# Membuat Spark Session
spark = (
    SparkSession.builder.appName("OLTP_process_job")
    .config(
        "spark.hadoop.fs.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
    )
    .config(
        "spark.hadoop.fs.AbstractFileSystem.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
    )
    .config("spark.hadoop.fs.gs.auth.service.account.enable", "true")
    .config(
        "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
        "/home/cekson/gcloud_gcs.json",
    )
    .getOrCreate()
)

# Koneksi ke database PostgreSQL
username = "postgres"
password = "postgres"
host = "192.168.51.118"
port = "5432"
database = "de_final_project"

jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
connection_properties = {
    "user": username,
    "password": password,
    "driver": "org.postgresql.Driver",
}

# Memuat tabel dari database ke DataFrame Spark
earthquake_table = spark.read.jdbc(
    url=jdbc_url, table="earthquake", properties=connection_properties
)
demographic_table = spark.read.jdbc(
    url=jdbc_url, table="demographic", properties=connection_properties
)
historical_table = spark.read.jdbc(
    url=jdbc_url, table="historical", properties=connection_properties
)
infrastructure_table = spark.read.jdbc(
    url=jdbc_url, table="infrastructure", properties=connection_properties
)
geological_table = spark.read.jdbc(
    url=jdbc_url, table="geological", properties=connection_properties
)

# Transformasi Data pada Tabel Earthquake
earthquake_table = (
    earthquake_table.withColumn("region", split(col("place"), ",").getItem(1))
    .withColumn("detail", split(col("place"), ",").getItem(0))
    .drop("place")
)

# Mengubah tipe data 'latitude' dan 'longitude' menjadi DoubleType
earthquake_table = earthquake_table.withColumn(
    "latitude", col("latitude").cast(DoubleType())
).withColumn("longitude", col("longitude").cast(DoubleType()))

# Menghitung median untuk masing-masing kolom menggunakan percentile_approx
median_nst = earthquake_table.approxQuantile("nst", [0.5], 0.0)[0]
median_gap = earthquake_table.approxQuantile("gap", [0.5], 0.0)[0]
median_dmin = earthquake_table.approxQuantile("dmin", [0.5], 0.0)[0]
median_horizontal_error = earthquake_table.approxQuantile(
    "horizontal_error", [0.5], 0.0
)[0]
median_mag_error = earthquake_table.approxQuantile("mag_error", [0.5], 0.0)[0]
median_mag_nst = earthquake_table.approxQuantile("mag_nst", [0.5], 0.0)[0]

# Mengisi nilai kosong dengan median yang telah dihitung
earthquake_table = earthquake_table.na.fill(
    {
        "nst": median_nst,
        "gap": median_gap,
        "dmin": median_dmin,
        "horizontal_error": median_horizontal_error,
        "mag_error": median_mag_error,
        "mag_nst": median_mag_nst,
    }
)

earthquake_table = earthquake_table.join(
    demographic_table, on=["latitude", "longitude"], how="left"
)

earthquake_table = earthquake_table.join(
    infrastructure_table, on=["latitude", "longitude"], how="left"
)
earthquake_table = earthquake_table.join(
    geological_table, on=["latitude", "longitude"], how="left"
)
earthquake_table = earthquake_table.join(
    historical_table, on=["latitude", "longitude"], how="left"
)

# Membuat Tabel Dimensi
dim_location = (
    earthquake_table.select("latitude", "longitude", "detail", "region")
    .distinct()
    .withColumn("location_id", monotonically_increasing_id() + 1)
)

dim_time = (
    earthquake_table.select("time")
    .distinct()
    .withColumn("time_id", monotonically_increasing_id() + 1)
    .withColumn("year", year(col("time")))
    .withColumn("month", month(col("time")))
    .withColumn("day", dayofmonth(col("time")))
    .withColumn("hour", hour(col("time")))
)

dim_demographic = (
    demographic_table.select("population_density", "urbanization_level")
    .distinct()
    .withColumn("demographic_id", monotonically_increasing_id() + 1)
)
dim_infrastructure = (
    infrastructure_table.select("building_type", "construction_quality")
    .distinct()
    .withColumn("infrastructure_id", monotonically_increasing_id() + 1)
)
dim_geological = (
    geological_table.select("fault_line_nearby", "tectonic_plate")
    .distinct()
    .withColumn("geological_id", monotonically_increasing_id() + 1)
)
dim_historical = (
    historical_table.select("previous_earthquakes", "max_magnitude")
    .distinct()
    .withColumn("historical_id", monotonically_increasing_id() + 1)
)

dim_historical = dim_historical.withColumn(
    "max_magnitude", col("max_magnitude").cast(DoubleType())
)


fact_earthquake = earthquake_table
fact_earthquake = fact_earthquake.drop("detail", "region")

fact_earthquake = fact_earthquake.join(
    dim_location, on=["latitude", "longitude"], how="left"
)

# Join dengan tabel waktu
fact_earthquake = fact_earthquake.join(dim_time, on=["time"], how="left")

# Join dengan tabel demografi berdasarkan location_id
fact_earthquake = fact_earthquake.join(
    dim_demographic,
    on=["population_density", "urbanization_level"],
    how="left",
)

# Join dengan tabel infrastruktur berdasarkan location_id
fact_earthquake = fact_earthquake.join(
    dim_infrastructure,
    on=["building_type", "construction_quality"],
    how="left",
)

# Join dengan tabel geologi berdasarkan location_id
fact_earthquake = fact_earthquake.join(
    dim_geological,
    on=["fault_line_nearby", "tectonic_plate"],
    how="left",
)

# Join dengan tabel historis berdasarkan location_id
fact_earthquake = fact_earthquake.join(
    dim_historical,
    on=["previous_earthquakes", "max_magnitude"],
    how="left",
)

fact_earthquake =

 fact_earthquake.withColumn("earthquake_id", monotonically_increasing_id() + 1)

# Memilih kolom yang relevan
fact_earthquake = fact_earthquake.select(
    "earthquake_id",
    "time_id",
    "location_id",
    "demographic_id",
    "infrastructure_id",
    "geological_id",
    "historical_id",
    "depth",
    "mag",
    "mag_type",
    "nst",
    "gap",
    "dmin",
    "rms",
    "horizontal_error",
    "depth_error",
    "mag_error",
    "mag_nst",
)

fact_earthquake = fact_earthquake.withColumn("depth", col("depth").cast(DoubleType()))
fact_earthquake = fact_earthquake.withColumn(
    "horizontal_error", col("horizontal_error").cast(DoubleType())
)
fact_earthquake = fact_earthquake.withColumn(
    "depth_error", col("depth_error").cast(DoubleType())
)
fact_earthquake = fact_earthquake.withColumn(
    "mag_error", col("mag_error").cast(DoubleType())
)
fact_earthquake = fact_earthquake.withColumn("gap", col("gap").cast(DoubleType()))
fact_earthquake = fact_earthquake.withColumn("dmin", col("dmin").cast(DoubleType()))
fact_earthquake = fact_earthquake.withColumn("rms", col("rms").cast(DoubleType()))

# Menyimpan DataFrames ke GCS dalam format Parquet
dim_location.write.mode("overwrite").parquet(
    f"gs://de_final_project/dim_location.parquet"
)
dim_time.write.mode("overwrite").parquet(f"gs://de_final_project/dim_time.parquet")
dim_demographic.write.mode("overwrite").parquet(
    f"gs://de_final_project/dim_demographic.parquet"
)
dim_infrastructure.write.mode("overwrite").parquet(
    f"gs://de_final_project/dim_infrastructure.parquet"
)
dim_geological.write.mode("overwrite").parquet(
    f"gs://de_final_project/dim_geological.parquet"
)
dim_historical.write.mode("overwrite").parquet(
    f"gs://de_final_project/dim_historical.parquet"
)
fact_earthquake.write.mode("overwrite").parquet(
    f"gs://de_final_project/fact_earthquake.parquet"
)

# Menghentikan Spark Session
spark.stop()
```

#### b. Kode Airflow untuk Orkestrasi ETL

```python
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

from datetime import datetime, timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

tables = [
    "dim_location",
    "dim_time",
    "dim_demographic",
    "dim_infrastructure",
    "dim_geological",
    "dim_historical",
    "fact_earthquake",
]

default_args = {
    "owner": "daniel",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "etl_earthquake_dataset",
    default_args=default_args,
    description="ETL earthquake dataset from csv >> pentaho >> spark >> gcs >> bigquery using airflow",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
)

start = DummyOperator(
    task_id="start",
    dag=dag,
)

finish = DummyOperator(
    task_id="finish",
    dag=dag,
)

run_pentaho_job = BashOperator(
    task_id="run_pentaho_job",
    bash_command="bash /home/cekson/run_pan.sh ",
    dag=dag,
)

run_spark_job = SparkSubmitOperator(
    task_id="run_spark_job",
    application="/home/cekson/airflow/spark-jobs/spark_oltp_process.py",
    conn_id="spark_default",
    total_executor_cores="2",
    executor_cores="1",
    executor_memory="2g",
    num_executors="2",
    driver_memory="2g",
    verbose=False,
    dag=dag,
)

for table in tables:
    load_gcs_to_bq = GCSToBigQueryOperator(
        task_id=f"load_gcs_to_bq_{table}",
        bucket="de_final_project",
        source_objects=[f"{table}.parquet/*"],
        destination_project_dataset_table=f"practice-413515.de_final_project.{table}",
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        dag=dag,
    )

    start >> run_pentaho_job >> run_spark_job >> load_gcs_to_bq >> finish
```

### 5. Kesimpulan

Proyek ini berhasil melakukan proses ETL pada dataset gempa bumi dengan integrasi data dari beberapa sumber. Proses ini dilakukan dengan menggunakan berbagai alat dan teknologi yang diatur oleh Apache Airflow untuk memastikan proses berjalan secara otomatis dan terjadwal. Data yang sudah diproses disimpan dalam Google BigQuery untuk analisis lebih lanjut.