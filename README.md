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
│
├── airflow/                # Contains Airflow DAGs and scripts
├── spark_jobs/             # Spark scripts for ETL processes
├── kafka/                  # Kafka producer and consumer scripts
├── aws_scripts/            # Scripts for AWS Glue, Athena, and Redshift
├── models/                 # Machine learning models for price prediction
├── data/
│   ├── raw/                # Raw data collected from the web
│   ├── processed/          # Cleaned and transformed data
│
├── image_for_project/      # Architecture diagrams and images
├── utils/                  # Utility scripts (e.g., helper functions, config management)
├── README.md               # Project documentation
└── requirements.txt        # Python dependencies
```

---

## **Pipeline Architecture**

<p align="center">
  <img src="https://github.com/trgtanhh04/Mobile-AWS-Pipeline-Engineering/blob/main/image_for_project/Data_Pipeline_Achitechture.png" width="100%" height="150%" alt="Pipeline Architecture">
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

### **Steps**
1. Clone the repository:
   ```bash
   git clone https://github.com/trgtanhh04/Mobile-AWS-Pipeline-Engineering.git
   cd Mobile-AWS-Pipeline-Engineering
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Start Airflow:
   ```bash
   airflow db init
   airflow scheduler
   airflow webserver
   ```
4. Start Kafka:
   ```bash
   bin/kafka-server-start.sh config/server.properties
   ```
5. Run the pipeline:
   - Trigger DAGs in Airflow to execute the pipeline.

---

## **Future Work**
1. **Enhance Machine Learning Models**:
   - Improve the accuracy of price prediction and recommendation systems.
2. **Integrate Real-time Features**:
   - Use streaming data pipelines for near real-time recommendations.
3. **Expand Data Sources**:
   - Include additional data sources to enrich the dataset.
4. **Improve System Scalability**:
   - Use Kubernetes to orchestrate Spark, Kafka, and other services for better scalability.

---

## **Contributing**
Contributions are welcome! Please follow these steps:
1. Fork the repository.
2. Create a new branch:
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. Commit your changes and push:
   ```bash
   git commit -m "Add your message"
   git push origin feature/your-feature-name
   ```
4. Submit a pull request.

---

## **License**
This project is licensed under the MIT License. See the `LICENSE` file for more details.
