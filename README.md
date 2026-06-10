# SDGs15-bigdata-itera
# Analisis Tutupan Lahan Berbasis *Big Data Pipeline* untuk Pemantauan Ekosistem Daratan Menggunakan Arsitektur *Medallion*

<p align="center">
  <img src="https://img.shields.io/badge/Apache%20Spark-3.5.8-orange?style=for-the-badge&logo=apachespark&logoColor=white" />
  <img src="https://img.shields.io/badge/Python-3.12-3776AB?style=for-the-badge&logo=python&logoColor=white" />
  <img src="https://img.shields.io/badge/OpenCV-5C3EE8?style=for-the-badge&logo=opencv&logoColor=white" />
  <img src="https://img.shields.io/badge/Windows_11-Local_Mode-0078D4?style=for-the-badge&logo=windows&logoColor=white" />
</p>

<p align="center">
  <b>Tugas Besar Analisis Big Data — Institut Teknologi Sumatera 2026</b><br>
  <i>Implementasi Medallion Architecture menggunakan PySpark Lokal untuk Analisis Citra Satelit DeepGlobe</i>
</p>

---

## 📋 Daftar Isi

- [Deskripsi Proyek](#-deskripsi-proyek)
- [Arsitektur Sistem](#-arsitektur-sistem)
- [Teknologi](#-teknologi)
- [Dataset](#-dataset)
- [Cara Menjalankan](#-cara-menjalankan)
- [Struktur Folder](#-struktur-folder)
- [Pipeline Medallion](#-pipeline-medallion)
- [Pemodelan & Evaluasi](#-pemodelan--evaluasi)

---

## 🎯 Deskripsi Proyek

Proyek ini merancang dan mengimplementasikan sistem pemrosesan data berbasis **Apache Spark** untuk melakukan klasifikasi citra satelit pada tingkat piksel (segmentasi semantik). Proyek ini menggunakan **Medallion Architecture** (Bronze → Silver → Gold) untuk membangun *pipeline* data yang terstruktur, bersih, dan efisien. 

Sistem ini dijalankan secara mandiri (*Local Standalone Mode*) di lingkungan Windows 11 tanpa menggunakan klasterisasi terdistribusi. Data diproses untuk melatih model **Machine Learning (Random Forest Classifier)** yang dapat mengenali 5 kelas tutupan lahan: Perkotaan (*Urban*), Pertanian (*Agriculture*), Lahan Terbuka (*Rangeland*), Perairan (*Water*), dan Tidak Diketahui (*Unknown/Barren*).

---

## 🏗️ Arsitektur Sistem

```text
┌─────────────────────────────────────────────────────────────────────────┐
│                     LOCAL WINDOWS ENVIRONMENT                           │
│  ┌─────────────────┐                                                    │
│  │ PySpark (Local) │                                                    │
│  │ Driver & Worker │                                                    │
│  └───────┬─────────┘                                                    │
│          │                                                              │
│          ▼                                                              │
│  ┌─────────────────┐                                                    │
│  │ LOCAL STORAGE   │                                                    │
│  │ D:/ABD_Tubes/   │                                                    │
│  │ ├── data_DeepGlobe/ ← Citra Asli (.jpg) & Masker (.png)              │
│  │ ├── bronze_layer/   ← Data Mentah RGB (Parquet)                      │
│  │ ├── silver_layer/   ← Fitur RGB + Label Numerik (Parquet)            │
│  │ └── gold_layer/     ← Vektor Fitur Siap Latih (Parquet)              │
│  └─────────────────┘                                                    │
└─────────────────────────────────────────────────────────────────────────┘
