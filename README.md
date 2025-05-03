# Project: AWS Data Pipeline for Mobile Recommendations

This project demonstrates the application of distributed data engineering principles to solve real-time data processing challenges. It integrates a variety of tools and technologies such as Apache Airflow, Apache Spark, Kafka, PostgreSQL, and AWS services (S3, Glue, Athena, Redshift). 

The ultimate goal is to create an application that provides professional users with personalized recommendations for mobile phones and predicts phone prices based on their attributes. The data is sourced from the website [MobileCity](https://mobilecity.vn/).

---

## **Objectives**
1. **Optimize Data Processing**:
   - Achieve a performance boost of **30-50%** in data processing time by leveraging Kafka's distributed messaging and AWS's scalable infrastructure.
2. **Predict Mobile Phone Prices**:
   - Use machine learning models to predict phone prices based on their attributes, enabling better decision-making for users.
3. **Develop Practical Applications**:
   - Create tangible applications that utilize the processed data for real-world use cases, like price prediction and personalized recommendations.
4. **Efficient Storage System on AWS**:
   - Build a centralized, scalable, and easily manageable storage system using AWS services.
5. **Automated ETL Pipelines**:
   - Design and implement fully automated ETL (Extract, Transform, Load) pipelines to streamline data processing.

---

## **Project Structure**

```
Mobile-AWS-Pipeline-Engineering/
├── config/                     # Configuration files for environment variables and app settings
│
├── dag/                        # Contains Airflow DAGs and related scripts
│   └── mobile_pipeline_dag.py  # Main Airflow DAG for orchestrating the pipeline
│
├── data/                       # Storage for data during the ETL process
│   ├── raw/                    # Unprocessed, raw data collected from sources
│   ├── staging/                # Intermediate data (from Kafka staging)
│   └── processed/              # Cleaned and transformed data ready for upstream applications
│
├── etl/                        # ETL scripts for data processing
│   ├── aws_etl.py              # ETL logic for AWS (e.g., uploading to S3, querying Athena)
│   └── mobile_etl.py           # ETL logic for mobile data (e.g., cleaning, feature engineering)
│
├── image_for_project/          # Visual assets for project documentation
│
├── notebooks/                  # Jupyter notebooks for exploratory data analysis (EDA) and modeling
│   ├── 1.1-data-exploring.ipynb             # Notebook for exploring trends in the data
│   ├── 1.2-data-visualizations.ipynb        # Notebook for advanced data visualizations
│   ├── 1.3-data-analyzing.ipynb             # Notebook for statistical and feature analysis
│   └── 1.4-data-modelling.ipynb             # Notebook for building predictive models
│
├── scripts/                    # Standalone scripts for specific tasks
│   ├── crawl_data.py           # Script for crawling data from MobileCity.vn
│   └── kafka_listening_staging.py # Script for Kafka consumer to handle staging data
│
├── utils/                      # Utility modules for reusable code
│   └── constants.py            # Constants and configuration keys used across the project
│
├── Dockerfile                  # Dockerfile for building the project container
├── docker-compose.yml          # Docker Compose file for managing multi-container setup
├── README.md                   # Project documentation
├── requirements.txt            # Python dependencies for the project
└── LICENSE                     # License for the project
```

---

## **Pipeline Architecture**

<p align="center">
  <img src="https://raw.githubusercontent.com/trgtanhh04/Mobile-AWS-Pipeline-Engineering/blob/main/imageForProject/Data_Pipeline_Achitechture.png" width="100%" alt="airflow">
</p>

The architecture consists of the following key components:
1. **Data Collection**:
   - Periodic web scraping to collect mobile phone data from [MobileCity](https://mobilecity.vn/).
2. **Data Storage**:
   - Raw data is first pushed into Kafka topics before being processed.
3. **Data Transformation**:
   - Spark jobs clean and transform the data using its structured API.
4. **Data Storage on AWS**:
   - Transformed data is stored in AWS S3 and made queryable via AWS Glue, Athena, and Redshift.

---

## **Task Dependencies (Airflow DAG)**

The following tasks are orchestrated and managed via Apache Airflow:
1. `run_crawl_data`: Periodic task to crawl data from the web.
2. `run_etl_mobile`: Spark job to clean and transform raw mobile data.
3. `run_etl_aws`: Task to upload processed data to AWS services for further analysis.

Dependency flow:
```
run_crawl_data -> run_etl_mobile -> run_etl_aws
```

---

## **Data Processing Workflow**

### **1. Data Collection**
- A crawler is scheduled to periodically scrape mobile phone data from multiple sources, ensuring comprehensive data coverage.
- The crawler is implemented using Python's `BeautifulSoup` and `Scrapy`.

### **2. Raw Data Storage**
- Instead of traditional data lakes like HDFS or PostgreSQL, raw data is stored in Kafka topics. This approach ensures:
  - Faster data retrieval times.
  - Better scalability for real-time data processing.

### **3. ETL Process**
- **Triggering ETL**:
  - Kafka consumers trigger the ETL process by fetching data from Kafka topics.
- **Data Cleaning**:
  - Apache Spark's Structured API is used to clean and transform the data.
  - Example transformations include handling missing values, normalizing text, and encoding categorical variables.

### **4. Uploading to AWS**
- **AWS S3**:
  - Cleaned data is first uploaded to an S3 bucket for staging.
- **Data Warehouse**:
  - Data in S3 is further processed and queried using:
    - **AWS Glue**: For cataloging and metadata management.
    - **AWS Athena**: For running SQL queries on S3 data.
    - **AWS Redshift**: As a data warehouse for advanced analytics and machine learning.

---

## **Machine Learning Application**

- **Goal**: Use the processed data to train machine learning models that provide:
  - Price predictions for mobile phones.
  - Personalized recommendations based on user preferences.
- **Tools Used**:
  - Scikit-learn and PySpark MLlib for model training and evaluation.
  - Model deployment using Flask or FastAPI.

---

## **Installation and Setup**

### **Prerequisites**
- Python 3.8 or higher
- Java 8 or higher (for Spark)
- Apache Kafka
- Apache Airflow
- AWS CLI configured with proper credentials
## **Setup Instructions**

Follow the steps below to set up and run the project:

### **1. Clone the Repository**
First, clone the repository to your local machine:
```bash
git clone https://github.com/trgtanhh04/Mobile-AWS-Pipeline-Engineering.git
cd Mobile-AWS-Pipeline-Engineering
```

---

### **2. Install Docker**
- **Download Docker Desktop**:
  - Go to the [Docker Desktop website](https://www.docker.com/products/docker-desktop/) and download the version suitable for your operating system.
  - Follow the instructions on the website to install Docker Desktop.
- **Verify Docker Installation**:
  - Run the following command to ensure Docker is installed and running:
    ```bash
    docker --version
    ```

---

### **3. Build the Docker Environment**
Use the `Dockerfile` and `docker-compose.yml` to set up the environment:
1. Build the Docker image:
   ```bash
   docker build -t mobile-aws-pipeline .
   ```
2. Start the services using Docker Compose:
   ```bash
   docker-compose up -d
   ```
   This will start all necessary containers, such as Apache Airflow, Kafka, and any other services defined in the `docker-compose.yml`.

---

### **4. Prerequisites**
Ensure you have the following prerequisites installed locally:
- **Java 11**:
  - Required for running Spark and other Java-based tools.
  - Verify installation:
    ```bash
    java -version
    ```
  - If not installed, download and install from [AdoptOpenJDK](https://adoptopenjdk.net/).
- **Python 3.8 or higher**:
  - Required for running the Python scripts and notebooks.
  - Verify installation:
    ```bash
    python --version
    ```
  - Install necessary Python dependencies:
    ```bash
    pip install -r requirements.txt
    ```

---

### **5. Set Up Airflow**
1. Initialize the Airflow database:
   ```bash
   airflow db init
   ```
2. Start the Airflow scheduler:
   ```bash
   airflow scheduler
   ```
3. Start the Airflow webserver:
   ```bash
   airflow webserver
   ```
4. Access the Airflow UI:
   - Open your browser and navigate to `http://localhost:8080`.

---

### **6. Start Kafka**
1. Start the Kafka service:
   ```bash
   docker exec -it kafka kafka-server-start.sh config/server.properties
   ```
2. Verify Kafka is running by listing topics:
   ```bash
   docker exec -it kafka kafka-topics.sh --list --zookeeper zookeeper:2181
   ```

---

### **7. Run the ETL Pipeline**
1. Trigger the Airflow DAG to execute the pipeline:
   - Go to the Airflow UI and activate the DAG named `mobile_pipeline_dag`.
2. Monitor the progress in the Airflow UI.

---

### **8. Access AWS Services**
- **S3 Buckets**:
  - Processed data will be uploaded to your configured S3 bucket.
- **Glue and Athena**:
  - Use AWS Glue to catalog the data and Athena to query it.
- **Redshift**:
  - Load the processed data into Redshift for further analysis.

---

### **9. Verify Everything**
Ensure all components (Airflow, Kafka, Spark, AWS) are running and integrated properly. Test the pipeline by processing a small dataset and verifying the output.

---

By following these steps, you can set up the project environment and start working on the data pipeline. For any issues or troubleshooting, refer to the relevant logs or documentation.e MIT License. See the `LICENSE` file for more details.
