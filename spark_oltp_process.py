from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    split,
    monotonically_increasing_id,
    year,
    month,
    dayofmonth,
    hour,
    expr,
)
from pyspark.sql.types import DoubleType

# Membuat Spark Session dengan konfigurasi JDBC Driver
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
    # demographic_table.select("latitude", "longitude", "population_density", "urbanization_level")
    .distinct()
    .withColumn("demographic_id", monotonically_increasing_id() + 1)
)
dim_infrastructure = (
    infrastructure_table.select("building_type", "construction_quality")
    # infrastructure_table.select("latitude", "longitude", "building_type", "construction_quality")
    .distinct()
    .withColumn("infrastructure_id", monotonically_increasing_id() + 1)
)
dim_geological = (
    geological_table.select("fault_line_nearby", "tectonic_plate")
    # geological_table.select("latitude", "longitude", "fault_line_nearby", "tectonic_plate")
    .distinct()
    .withColumn("geological_id", monotonically_increasing_id() + 1)
)
dim_historical = (
    historical_table.select("previous_earthquakes", "max_magnitude")
    # historical_table.select("latitude", "longitude", "previous_earthquakes", "max_magnitude")
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
# fact_earthquake = earthquake_table.join(
    dim_infrastructure,
    on=["building_type", "construction_quality"],
    # on=["latitude", "longitude"],
    # fact_earthquake["location_id"] == dim_infrastructure["infrastructure_id"],
    how="left",
)

# Join dengan tabel geologi berdasarkan location_id
fact_earthquake = fact_earthquake.join(
# fact_earthquake = earthquake_table.join(
    dim_geological,
    # fact_earthquake["location_id"] == dim_geological["geological_id"],
    on=["fault_line_nearby", "tectonic_plate"],
    # on=["latitude", "longitude"],
    how="left",
)

# Join dengan tabel historis berdasarkan location_id
fact_earthquake = fact_earthquake.join(
    dim_historical,
    on=["previous_earthquakes", "max_magnitude"],
    how="left",
)

fact_earthquake = fact_earthquake.withColumn("earthquake_id", monotonically_increasing_id() + 1)

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
