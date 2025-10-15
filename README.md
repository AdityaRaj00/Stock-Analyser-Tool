# 🧠 Interactive Stock Market Analysis Pipeline

![Python](https://img.shields.io/badge/Python-3.7%2B-blue?logo=python)
![Cloud](https://img.shields.io/badge/Cloud-GCP-yellow?logo=googlecloud)
![Data](https://img.shields.io/badge/Data-Pandas%20%7C%20PySpark-orange?logo=apache-spark)

A versatile, **interactive Python script** for fetching, storing, and analyzing historical stock and mutual fund data from the **Indian stock market (NSE/BSE)**.  
This project is built as a flexible **data engineering and analysis tool**, capable of running seamlessly in both **local** and **cloud (GCP/Databricks)** environments.

---

## 🚀 Features

- **🧭 Interactive Prompts**  
  Guides you through selecting tickers, time periods, and operation modes.

- **💾 Dual Storage Modes**
  - **Local:** Saves data in a partitioned folder on your local system.  
  - **Cloud (GCP):** Uploads data directly to a Google Cloud Storage (GCS) bucket.

- **⚙️ Dual Processing Modes**
  - **Local:** Analyzes data with **Pandas** and visualizes performance using **Matplotlib**.  
  - **Databricks:** Prepares data for distributed analysis and provides **ready-to-use PySpark code**.

- **📈 Dynamic Data Fetching**  
  Fetches historical stock or mutual fund data for user-specified tickers and time periods (Weekly, Monthly, Yearly, etc.) using the **`yfinance`** library.

---

<details>
<summary>🧩 <b>How to Run</b></summary>

### ✅ Prerequisites

- **Python 3.7+**
- Install dependencies:

```bash
pip install yfinance pandas matplotlib seaborn google-cloud-storage
```

---

### ⚙️ Configuration (Optional for GCP Users)

If you plan to use **Google Cloud Storage**, perform the following setup:

1. Create a **GCP Service Account** with the **Storage Admin** role.  
2. Download its **JSON key file**.  
3. Set the environment variable for authentication.

#### macOS / Linux
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/gcp_key.json"
```

#### Windows (Command Prompt)
```bash
set GOOGLE_APPLICATION_CREDENTIALS="C:\path\to\your\gcp_key.json"
```

4. Open `main.py` and replace the placeholder:
```python
GCS_BUCKET_NAME = "your-bucket-name"
```

---

### ▶️ Execution

Run the script from your terminal:

```bash
python main.py
```

Follow the on-screen prompts to:
- Choose your **storage mode** (local or GCP)
- Select your **processing mode** (local or Databricks)
- Enter your **stock ticker** and **time period**

</details>

---

## 🔄 Project Workflow

### **Phase 1: Data Ingestion**
1. User selects a stock ticker (e.g., `RELIANCE.NS`) and a time period.  
2. User chooses storage target (**local** or **GCP**).  
3. Script downloads and saves data in a partitioned format:
   ```
   /ticker=TICKER/date=YYYY-MM-DD.csv
   ```

### **Phase 2: Data Processing**
1. User selects a processing target (**local** or **databricks**).  
2. Based on the choice:
   - **Local Mode:**  
     Loads data using Pandas → Performs analysis → Generates price performance graph.
   - **Databricks Mode:**  
     Outputs a complete **PySpark code block**, ready to run in a Databricks notebook for scalable analysis.

---

## 📊 Example Use Cases
- Individual investors analyzing long-term trends.  
- Data engineers setting up automated stock data ingestion.  
- Analysts testing PySpark pipelines on real financial data.  

---

## 🧰 Tech Stack
| Component | Technology |
|------------|-------------|
| Programming Language | Python 3.7+ |
| Data Fetching | yfinance |
| Data Analysis | Pandas |
| Visualization | Matplotlib, Seaborn |
| Cloud Storage | Google Cloud Storage |
| Big Data Processing | Databricks, PySpark |

---

## 🏁 Future Improvements
- Integration with **BigQuery** for advanced analytics  
- Support for **real-time streaming data**  
- Addition of **automated report generation**

---

## 👨‍💻 Author & License

**Author:** [Aditya Raj Gaur](https://github.com/)  

> 💡 *Contributions, bug reports, and feature requests are welcome!*
