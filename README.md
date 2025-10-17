# ETL Pipeline: S3 → Data Processing → S3

This project implements an **end-to-end ETL (Extract, Transform, Load)** pipeline using **Python** and **AWS S3**.  
The pipeline ingests raw data from an input S3 bucket, performs data cleaning and transformation, and writes the cleaned data back to an output S3 bucket.  
It follows **Object-Oriented Programming (OOP)** design principles, includes **structured logging**, and provides **unit tests** for end-to-end validation.

---

## Features

- **Automated Data Extraction** from Amazon S3  
- **Data Transformation & Cleaning** using modular OOP design  
- **Data Load** back into S3 as clean output  
- **Centralized Logging** for traceability and error tracking  
- **Unit Testing with `unittest`** for component-level and end-to-end verification  
- **Configurable via `.yml`**  
- **Extensible** — easily add new transformations or destinations  


## 🧱 Architecture Overview
         
         ┌────────────────────┐
         │     Raw Data       │
         │    (S3 Bucket)     │
         └─────────┬──────────┘
                   │
                   ▼
         ┌────────────────────┐
         │   Extract Layer    │
         └─────────┬──────────┘
                   │
                   ▼
         ┌────────────────────┐
         │  Transform Layer   │
         └─────────┬──────────┘
                   │
                   ▼
         ┌────────────────────┐
         │     Load Layer     │
         └─────────┬──────────┘
                   │
                   ▼
         ┌────────────────────┐
         │   Clean Data (S3)   │
         └────────────────────┘
