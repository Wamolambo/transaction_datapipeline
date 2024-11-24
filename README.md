## Project Focus
This project simulates an ETL pipeline for a Data Engineer Technical Assessment. The goal of the assessment is to build an efficient pipeline that extracts, transforms, and loads customer transaction data into a MySQL database, making it available for analytics and insights.

An image of the ETL pipeline is shown below:

![image](img/etl.png)
## Setup Instructions
1. Clone the repository:
```bash
   git git@github.com:Wamolambo/transaction_datapipeline.git
   cd transaction_data_workflow
   ```
2. Install dependencies:
```bash
    python3 -m venv etl_env
    source etl_env/bin/activate  # Linux/Mac
    etl_env\Scripts\activate     # Windows
    pip install -r requirements.txt
```
3. Set up MySQL:
```bash
CREATE DATABASE planet42;
   ```

```bash
CREATE USER 'user'@'localhost' IDENTIFIED BY 'password';
GRANT ALL PRIVILEGES ON planet42_assessment.* TO 'user'@'localhost';
FLUSH PRIVILEGES;
   ```