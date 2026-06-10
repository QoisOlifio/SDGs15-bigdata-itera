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

---

## 📋 Daftar Isi

- [Deskripsi Proyek](#-deskripsi-proyek)
- [Anggota Tim](#-anggota-tim)
- [Arsitektur Sistem](#-arsitektur-sistem)
- [Teknologi](#-teknologi)
- [Dataset](#-dataset)
- [Cara Menjalankan](#-cara-menjalankan)
- [Struktur Folder](#-struktur-folder)
- [Pipeline Medallion](#-pipeline-medallion)
- [Benchmark & Evaluasi](#-benchmark--evaluasi)
- [Status Proyek](#-status-proyek)
- [Referensi](#-referensi)

---

## 🎯 Deskripsi Proyek

Proyek ini merancang dan mengimplementasikan **sistem pemrosesan data skala besar** untuk melakukan klasifikasi tutupan lahan (*land cover classification*) pada tingkat piksel (segmentasi semantik) menggunakan gambar satelit resolusi tinggi. 

Menggunakan **Medallion Architecture** (Bronze → Silver → Gold) yang dijalankan pada **Apache Spark Standalone Lokal**, sistem ini mengonversi matriks citra biner raksasa menjadi bentuk tabular terdistribusi, melakukan rekayasa fitur warna (RGB), dan melatih algoritma **Random Forest Classifier** guna memetakan 5 kelas tutupan lahan secara efisien tanpa terkendala batasan memori komputasi konvensional.

### Pertanyaan Ilmiah
> *Apakah arsitektur Medallion berbasis Apache Spark dalam mode Lokal Standalone mampu mengoptimalkan throughput pemrosesan piksel citra satelit resolusi tinggi dan meminimalkan latensi end-to-end training model Random Forest dibandingkan dengan pemrosesan sekuensial berbasis matriks NumPy/Pandas konvensional?*

---

## 👥 Anggota Tim

| No | Nama | Peran | Tanggung Jawab Utama |
|:---:|:---|:---|:---|
| 1 | **[Nama Ketua Anda]** | 🎯 Ketua / Project Integrator | Koordinasi harian, penyusunan repositori, finalisasi proposal & laporan, integrasi modul |
| 2 | **[Nama Anggota 2]** | 🗃️ Data Engineer | Ingesti gambar mentah, translasi matriks piksel, membangun pipeline Bronze → Silver layer |
| 3 | **[Nama Anggota 3]** | ⚡ Spark Analytics Developer | Rekayasa fitur Gold layer (VectorAssembler), tuning hyperparameter Random Forest |
| 4 | **[Nama Anggota 4]** | 📊 Baseline & Benchmark Specialist | Membuat pemrosesan sekuensial pembanding, mengukur rasio throughput & latensi Spark vs Pandas |
| 5 | **[Nama Anggota 5]** | 🖥️ Environment & DevOps Specialist | Konfigurasi Environment Variables (Hadoop/Spark), manajemen alokasi RAM Driver, optimasi IO disk |

---

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

# 🌍 DeepGlobe Land Cover Classification Pixel-Level Pipeline

Projek ini berfokus pada pembangunan *data pipeline* performa tinggi untuk mengekstraksi, memproses, dan memetakan data berbasis piksel (*pixel-level records*) dari citra satelit **DeepGlobe Land Cover Dataset**. Menggunakan **Apache Spark** untuk pemrosesan paralel lokal dan format **Apache Parquet** untuk efisiensi penyimpanan skala besar.

---

## 🛠️ Teknologi & Arsitektur Sistem

Berikut adalah komponen teknologi yang digunakan dalam mendesain *pipeline* ini:

| Kategori | Teknologi | Versi | Fungsi |
| :--- | :--- | :--- | :--- |
| **Big Data Engine** | Apache Spark | 3.5.8 | Pemrosesan paralel lokal & manajemen DataFrame terdistribusi. |
| **Hadoop Windows Utility** | winutils | 3.0.0 | Jembatan sistem berkas Windows agar kompatibel dengan API HDFS. |
| **Bahasa Pemrograman** | Python | 3.12 | Bahasa utama penulisan skrip *pipeline* & algoritma Machine Learning. |
| **Computer Vision** | OpenCV | *Latest* | Library pembacaan citra satelit dan ekstraksi matriks warna BGR/RGB. |
| **Format Penyimpanan** | Apache Parquet | — | Penyimpanan data kolumnar terkompresi untuk memangkas I/O disk. |

---

## 📊 Detail Dataset

Pemrosesan dilakukan hingga tingkat piksel individual untuk menangkap detail klasifikasi lahan secara presisi.

### Spesifikasi Data
* **Nama Dataset:** DeepGlobe Land Cover Classification Dataset
* **Unit Observasi:** Piksel Individual Citra Satelit (*pixel-level records*)
* **Estimasi Volume Data:** **~4.194.304 baris** (Dihasilkan dari per satu pasang gambar resolusi $2048 \times 2048$)

### Skema Ekstraksi Masker Warna
Setiap piksel diklasifikasikan ke dalam kategori lahan berdasarkan nilai matriks warna berikut:

| Kode Warna (BGR/RGB) | Kategori Lahan / Kelas | Target Visual |
| :--- | :--- | :--- |
| `[0, 255, 255]` | Urban | Kuning / Cyan |
| `[255, 255, 0]` | Agriculture | Kuning |
| `[255, 0, 255]` | Rangeland | Magenta |
| `[0, 0, 255]` | Water | Biru |
| `[255, 255, 255]` | Unknown | Putih |

---
