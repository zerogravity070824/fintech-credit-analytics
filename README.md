# 🏦 Fintech Credit Analytics: Scalable Data Pipeline for Instant Loan Approval

## 📌 Project Overview
Proyek ini adalah simulasi *end-to-end data pipeline* untuk institusi finansial (Fintech/Multi-finance). Pipeline ini memproses ratusan ribu data riwayat kredit mentah menjadi model data analitik yang siap digunakan untuk divisualisasikan oleh tim *Risk* dan *Business Intelligence*.

**Business Value:**
Pipeline otomatis ini dibangun untuk mengurangi waktu pemrosesan data historis nasabah. Dengan pemodelan data terpusat, tim *Risk Analyst* dapat memantau performa kredit (Approval Rate & Default Rate) secara harian dan menghemat waktu *query* manual hingga 40%, memungkinkan pengambilan keputusan kredit yang lebih cepat dan akurat.

---

## 🛠️ Tech Stack & Architecture
* **Cloud Data Warehouse:** Google BigQuery
* **Data Transformation:** dbt (Data Build Tool) Core
* **Orchestration & CI/CD:** GitHub Actions
* **Visualization / BI:** Looker Studio
* **Language:** SQL, Python, YAML

*(Catatan untuk Ilham: Nanti kamu bisa menambahkan screenshot gambar Arsitektur Data kamu di sini)*

---

## 🏗️ Data Modeling Approach (Star Schema)
Proyek ini mengadopsi metodologi pemodelan data modular menggunakan dbt dengan pendekatan *Star Schema*:

1. **Staging Layer (`stg_`):** Membersihkan data mentah, standarisasi format penamaan kolom menjadi *snake_case*, dan menyesuaikan tipe data.
2. **Intermediate Layer (`int_`):** Melakukan *JOIN* kompleks antara data profil nasabah (`application_train`) dengan riwayat BI Checking eksternal (`bureau`), serta menghitung agregasi metrik.
3. **Marts Layer (`dim_`, `fct_`, `obt_`):** Membangun *Dimension tables*, *Fact tables*, dan akhirnya denormalisasi menjadi **One Big Table (`obt_credit_risk`)** yang dioptimalkan khusus untuk performa *dashboard* Looker Studio.

---

## 🤖 CI/CD & Orchestration
Pipeline ini berjalan secara otomatis menggunakan **GitHub Actions** dengan penerapan *Continuous Integration* dan *Continuous Deployment* (CI/CD):
* **Automated Testing:** Setiap perubahan kode akan memicu perintah `dbt test` untuk memastikan kualitas data (tidak ada nilai *null* pada *Primary Key*, tidak ada duplikasi) sebelum masuk ke *production*.
* **Daily Batch Processing:** Menjalankan perintah `dbt run` secara otomatis setiap jam 06:00 WITA menggunakan pengaturan *cron job* untuk me-*refresh* tabel di BigQuery.

---

## 📊 Dashboard & Visualization
Data yang telah dimodelkan kemudian dihubungkan ke Looker Studio untuk memantau metrik utama portofolio kredit.

🔗 **[Klik di sini untuk melihat Interaktif Dashboard Looker Studio](https://lookerstudio.google.com/reporting/30bb83de-cf37-4263-a1d2-b05fccebd04b)**

*Metrics Tracked:*
* Default Rate (Nasabah Lancar vs Macet)
* Portfolio Distribution by Income Type
* Total Applicants

---
*Created by Ilham - 2026*