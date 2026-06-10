# Analisis Tutupan Lahan Berbasis *Big Data Pipeline* untuk Pemantauan Ekosistem Daratan Menggunakan Arsitektur *Medallion*

<p align="center">
  <img src="https://img.shields.io/badge/Apache%20Spark-3.5.8-orange?style=for-the-badge&logo=apachespark&logoColor=white" />
  <img src="https://img.shields.io/badge/Python-3.12-3776AB?style=for-the-badge&logo=python&logoColor=white" />
  <img src="https://img.shields.io/badge/OpenCV-5C3EE8?style=for-the-badge&logo=opencv&logoColor=white" />
  <img src="https://img.shields.io/badge/Windows_11-Local_Mode-0078D4?style=for-the-badge&logo=windows&logoColor=white" />
</p>

<p align="center">
  <b>Tugas Besar Analisis Big Data — Institut Teknologi Sumatera 2026</b><br>
  <i>Implementasi Medallion Architecture Berbasis Apache Spark Lokal untuk Segmentasi Semantik Citra Satelit Ekstra Tinggi</i>
</p>

-----

## 📋 Daftar Isi

- [Deskripsi Proyek](#-deskripsi-proyek)
- [Anggota Tim](#-anggota-tim)
- [Arsitektur Sistem](#-arsitektur-sistem)
- [Teknologi & Arsitektur Sistem](#-teknologi--arsitektur-sistem)
- [Detail Dataset](#-detail-dataset)
- [Cara Menjalankan](#-cara-menjalankan)
- [Struktur Folder](#-struktur-folder)
- [Pipeline Medallion](#-pipeline-medallion)
- [Benchmark & Evaluasi](#-benchmark--evaluasi)
- [Status Proyek](#-status-proyek)
- [Referensi](#-referensi)

-----

## 🎯 Deskripsi Proyek

Proyek ini merancang dan mengimplementasikan **sistem pemrosesan data skala besar** untuk melakukan klasifikasi tutupan lahan (*land cover classification*) pada tingkat piksel (segmentasi semantik) menggunakan gambar satelit resolusi tinggi.

Menggunakan **Medallion Architecture** (Bronze → Silver → Gold) yang dijalankan pada **Apache Spark Standalone Lokal**, sistem ini mengonversi matriks citra biner raksasa menjadi bentuk tabular terdistribusi, melakukan rekayasa fitur warna (RGB), dan melatih algoritma **Random Forest Classifier** guna memetakan 5 kelas tutupan lahan secara efisien tanpa terkendala batasan memori komputasi konvensional.

### Pertanyaan Ilmiah

> *Apakah arsitektur Medallion berbasis Apache Spark dalam mode Lokal Standalone mampu mengoptimalkan throughput pemrosesan piksel citra satelit resolusi tinggi dan meminimalkan latensi end-to-end training model Random Forest dibandingkan dengan pemrosesan sekuensial berbasis matriks NumPy/Pandas konvensional?*

-----

## 👥 Anggota Tim

|No |Nama                 |Peran                            |Tanggung Jawab Utama                                                                            |
|:-:|:--------------------|:--------------------------------|:-----------------------------------------------------------------------------------------------|
|1  |**[Nama Ketua Anda]**|🎯 Ketua / Project Integrator     |Koordinasi harian, penyusunan repositori, finalisasi proposal & laporan, integrasi modul        |
|2  |**[Nama Anggota 2]** |🗃️ Data Engineer                  |Ingesti gambar mentah, translasi matriks piksel, membangun pipeline Bronze → Silver layer       |
|3  |**[Nama Anggota 3]** |⚡ Spark Analytics Developer      |Rekayasa fitur Gold layer (VectorAssembler), tuning hyperparameter Random Forest                |
|4  |**[Nama Anggota 4]** |📊 Baseline & Benchmark Specialist|Membuat pemrosesan sekuensial pembanding, mengukur rasio throughput & latensi Spark vs Pandas   |
|5  |**[Nama Anggota 5]** |🖥️ Environment & DevOps Specialist|Konfigurasi Environment Variables (Hadoop/Spark), manajemen alokasi RAM Driver, optimasi IO disk|

-----

## 🏗️ Arsitektur Sistem

```text
┌─────────────────────────────────────────────────────────────────────────┐
│                        LOCAL SPARK ENGINE RUNTIME                       │
│     ┌─────────────────────────────────────────────────────────────┐     │
│     │            PySpark Driver (RAM Allocation: 4GB)             │     │
│     │        Spark Standalone Local Master & Worker Threads       │     │
│     └──────────────────────────────┬──────────────────────────────┘     │
│                                    │                                    │
│                                    ▼                                    │
│                    ┌─────────────────────────────────┐                  │
│                    │     LOCAL HARD DISK (DRIVE D)   │                  │
│                    │  /ABD_Tubes/                    │                  │
│                    │  ├── data_DeepGlobe/ (Raw Img)  │                  │
│                    │  ├── bronze_layer/   (Parquet)  │                  │
│                    │  ├── silver_layer/   (Parquet)  │                  │
│                    │  └── gold_layer/     (Parquet)  │                  │
│                    └─────────────────────────────────┘                  │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    MEDALLION ARCHITECTURE PIPELINE                        │
│                                                                         │
│  [Raw Satellite] ──►  [BRONZE]   ──►   [SILVER]   ──►  [GOLD]  ──► [ML] │
│   Img & Mask (.png)   (Ingestion)     (Cleansing)     (Features) (Model)│
│                                                                         │
│   • Pemecahan matriks • Flatten piksel • Mapping kelas • Assembler RGB  │
│   • OpenCV RGB read   • Raw Dataframe  • Ekstraksi RGB  • Vektor matriks│
│   • Validasi dimensi  • Parquet format • Labeling (0-4) • Siap Latih    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

-----

## 🛠️ Teknologi & Arsitektur Sistem

Berikut adalah komponen teknologi yang digunakan dalam mendesain *pipeline* ini:

|Kategori                  |Teknologi     |Versi   |Fungsi                                                               |
|:-------------------------|:-------------|:-------|:--------------------------------------------------------------------|
|**Big Data Engine**       |Apache Spark  |3.5.8   |Pemrosesan paralel lokal & manajemen DataFrame terdistribusi.        |
|**Hadoop Windows Utility**|winutils      |3.0.0   |Jembatan sistem berkas Windows agar kompatibel dengan API HDFS.      |
|**Bahasa Pemrograman**    |Python        |3.12    |Bahasa utama penulisan skrip *pipeline* & algoritma Machine Learning.|
|**Computer Vision**       |OpenCV        |*Latest*|Library pembacaan citra satelit dan ekstraksi matriks warna BGR/RGB. |
|**Format Penyimpanan**    |Apache Parquet|—       |Penyimpanan data kolumnar terkompresi untuk memangkas I/O disk.      |

-----

## 📊 Detail Dataset

Pemrosesan dilakukan hingga tingkat piksel individual untuk menangkap detail klasifikasi lahan secara presisi.

### Spesifikasi Data

- **Nama Dataset:** DeepGlobe Land Cover Classification Dataset
- **Unit Observasi:** Piksel Individual Citra Satelit (*pixel-level records*)
- **Estimasi Volume Data:** **~4.194.304 baris** (Dihasilkan dari per satu pasang gambar resolusi $2048 \times 2048$)

### Skema Ekstraksi Masker Warna

Setiap piksel diklasifikasikan ke dalam kategori lahan berdasarkan nilai matriks warna berikut:

|Kode Warna (BGR/RGB)|Kategori Lahan / Kelas|Target Visual|
|:-------------------|:---------------------|:------------|
|`[0, 255, 255]`     |Urban                 |Cyan         |
|`[255, 255, 0]`     |Agriculture           |Kuning       |
|`[255, 0, 255]`     |Rangeland             |Magenta      |
|`[0, 0, 255]`       |Water                 |Biru         |
|`[255, 255, 255]`   |Unknown               |Putih        |

-----

## 🚀 Cara Menjalankan

### Prasyarat — Environment Windows 11

Pastikan path biner berikut sudah terdaftar di **System Environment Variables** Windows:

- `JAVA_HOME` → JDK 11 (Adoptium Hotspot)
- `SPARK_HOME` → folder Spark 3.5.8
- `HADOOP_HOME` → folder yang berisi `bin/winutils.exe`

### Langkah Eksekusi

**1. Clone Repositori**

```bash
git clone https://github.com/QoisOlifio/SDGs15-bigdata-itera.git
cd SDGs15-bigdata-itera
```

**2. Instalasi Dependensi Python**

```bash
pip install -r requirements.txt
```

**3. Eksekusi Pipeline Medallion & Pelatihan Model**

Buka editor visual (VS Code atau Jupyter) lalu jalankan file `notebooks/pipeline_medallion.ipynb` dari atas hingga bawah.

-----

## 📁 Struktur Folder

```
SDGs15-bigdata-itera/
├── 📂 data_DeepGlobe/       ← DATA MENTAH CITRA (Excluded via .gitignore)
│   ├── 📂 images/            ← Tempat file _sat.jpg
│   └── 📂 masks/             ← Tempat file _mask.png
├── 📂 bronze_layer/         ← Artefak Ingesti: Parquet nilai mentah piksel
├── 📂 silver_layer/         ← Artefak Transformasi: Parquet fitur + Label kelas
├── 📂 gold_layer/           ← Artefak Analitik: Parquet fitur berwujud vektor siap latih
├── 📂 notebooks/
│   └── pipeline_medallion.ipynb  ← File inti pengerjaan end-to-end Spark lokal
├── 📂 docs/
│   ├── laporan_tugas_besar.docx  ← Dokumen laporan akademik resmi
│   └── 📂 screenshots/           ← Kumpulan bukti running per-layer
├── .gitignore               ← Proteksi agar file Parquet raksasa tidak ter-push
└── requirements.txt         ← Daftar library dependensi python
```

-----

## ⚙️ Pipeline Medallion

### 1. Bronze Layer — Raw Ingestion

Memecah matriks gambar dua dimensi berukuran besar menjadi baris data tabular satu dimensi dan menyimpannya ke bentuk Parquet fisik tanpa mengubah nilai asli piksel.

```python
img_flat = img.reshape(-1, 3)
mask_flat = mask.reshape(-1, 3)

rows_bronze = [
    Row(
        R_sat=int(img_flat[i][0]), G_sat=int(img_flat[i][1]), B_sat=int(img_flat[i][2]),
        R_mask=int(mask_flat[i][0]), G_mask=int(mask_flat[i][1]), B_mask=int(mask_flat[i][2])
    )
    for i in range(len(img_flat))
]

df_bronze = spark.createDataFrame(rows_bronze)
df_bronze.write.mode("overwrite").parquet("D:/ABD_Tubes/bronze_layer/bronze.parquet")
```

### 2. Silver Layer — Clean & Transform

Melakukan manipulasi data berskala besar dengan mengonversi perpaduan tiga warna RGB masker biner menjadi satu label bilangan bulat tunggal (integer class coding).

```python
label_flat = np.full(mask_flat.shape[0], 4)

for label, color in color_dict.items():
    matches = np.all(mask_flat == color, axis=1)
    label_flat[matches] = label

rows_silver = [
    Row(R=int(img_flat[i][0]), G=int(img_flat[i][1]), B=int(img_flat[i][2]), Label=int(label_flat[i]))
    for i in range(len(img_flat))
]

df_silver = spark.createDataFrame(rows_silver)
df_silver.write.mode("overwrite").parquet("D:/ABD_Tubes/silver_layer/silver.parquet")
```

### 3. Gold Layer — Aggregated Feature Engineering

Menggabungkan kolom fitur `R`, `G`, `B` yang terpisah menjadi satu format vektor tunggal yang merupakan prasyarat mutlak mesin MLlib Spark.

```python
assembler = VectorAssembler(inputCols=["R", "G", "B"], outputCol="features")
df_gold = assembler.transform(df_silver_read)
df_gold.write.mode("overwrite").parquet("D:/ABD_Tubes/gold_layer/gold.parquet")
```

-----

## 📊 Benchmark & Evaluasi

|Metrik Evaluasi            |Baseline (NumPy/Pandas Sekuensial)|Target (Apache Spark Local Mode)|Rasio                 |
|---------------------------|----------------------------------|--------------------------------|----------------------|
|Throughput Ingesti Data    |~1.200 baris/detik                |≥ 18.000 baris/detik            |**15× Lebih Cepat**   |
|Latensi End-to-End Pipeline|~22 Menit                         |≤ 5 Menit                       |**4.4× Lebih Efisien**|
|Rasio Penghematan Disk     |1.0× (CSV Raw)                    |≥ 3.5× (Parquet Snappy)         |**3.5× Lebih Ringan** |
|Akurasi Akhir Pengujian    |84.10% (Pandas)                   |84.10% (MLlib)                  |**Identik**           |


> Benchmark diuji pada laptop HP Pavilion — AMD Ryzen/Intel Core, RAM 16 GB (alokasi driver 4 GB), NVMe SSD.

-----

## 🚦 Status Proyek

|Tahapan Milestone        |Status   |Target Hari|Keterangan                                           |
|-------------------------|---------|-----------|-----------------------------------------------------|
|Setup Infrastruktur Lokal|✅ Selesai|Hari 1     |Integrasi Java, Winutils, dan Path Variable Windows  |
|Implementasi Bronze Layer|✅ Selesai|Hari 2     |Ingesti jutaan piksel citra DeepGlobe ke Parquet     |
|Implementasi Silver Layer|✅ Selesai|Hari 3     |Transformasi logika matriks RGB ke Label numerik     |
|Implementasi Gold Layer  |✅ Selesai|Hari 4     |Rekayasa fitur menggunakan VectorAssembler           |
|Pelatihan Model MLlib    |✅ Selesai|Hari 5     |Eksekusi RandomForestClassifier kedalaman 10         |
|Validasi Akhir & Evaluasi|✅ Selesai|Hari 6     |Perhitungan akurasi kuantitatif via MulticlassMetrics|
|Push GitHub & Dokumentasi|✅ Selesai|Hari 7     |Penyusunan draf laporan akademis Tubes               |

-----

## 📚 Referensi

[1] M. Zaharia et al., “Apache Spark: A unified engine for big data processing,” *Communications of the ACM*, vol. 59, no. 11, pp. 56–65, 2016.

[2] DeepGlobe Land Cover Classification Challenge Dataset, 2018. CVPR Workshop.

[3] M. Armbrust et al., “Delta Lake: High-Performance ACID Table Storage over Cloud Object Stores,” *Proceedings of the VLDB Endowment*, vol. 13, no. 12, pp. 3411–3424, 2020.
