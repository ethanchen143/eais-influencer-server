## **Table of Contents**
1. [Python Dependencies](#python-dependencies)  
2. [Setting Up PostgreSQL Locally](#setting-up-postgresql-locally)  
   - [Installation](#installation)  
   - [Creating Users and Databases](#creating-users-and-databases)  
3. [Handling Data Migration](#handling-data-migration)  
4. [Running and Testing the API](#running-and-testing-the-api)  
5. [Importing Data](#importing-data)

---

## **Python Dependencies**  
Ensure you have the following Python packages installed:  
```bash
pip install flask flask_sqlalchemy flask_migrate
```

---

## **Setting Up PostgreSQL Locally**

### **Installation**
For macOS, you can install PostgreSQL with Homebrew:
```bash
brew update
brew install postgresql
brew services start postgresql
```

### **Creating Users and Databases**
1. **Create a PostgreSQL Superuser:**
   ```bash
   createuser -s EAIS
   ```

2. **Create the Database:**
   ```bash
   createdb EAIS
   ```

3. **Activate the PostgreSQL Command Line Interface:**
   ```bash
   psql -U EAIS
   ```

4. **Create a New Role (User):**
   ```sql
   CREATE ROLE your_username WITH LOGIN PASSWORD 'your_password';
   ```

5. **Create a New Database:**
   ```sql
   CREATE DATABASE tiktok_influencers_db;
   ```

6. **Grant Privileges to the User:**
   ```sql
   GRANT ALL PRIVILEGES ON DATABASE tiktok_influencers_db TO your_username;
   ```

7. **Exit PostgreSQL CLI:**
   ```bash
   \q
   ```

   **PostgreSQL 15+ Users:**  
   Run the following before exiting to ensure schema access:
   ```sql
   GRANT ALL ON SCHEMA public TO your_username;
   ```

8. **Test the Connection:**
   ```bash
   psql -U your_username -d tiktok_influencers_db -h localhost
   ```
   Exit with `\q` when done.

---

## **Handling Data Migration**

1. **Initialize the Migration Repository:**
   ```bash
   flask db init
   ```

   *(If the above fails, try using: `python3 flask db init`)*

2. **Create and Apply Migrations:**
   ```bash
   FLASK_APP=manage.py flask db migrate -m "Initial migration."
   FLASK_APP=manage.py flask db upgrade
   ```

---

## **Running and Testing the API**

1. **Start the Server:**
   ```bash
   python3 manage.py
   ```

2. **Test Endpoints using `curl`:**
   Example POST request:
   ```bash
   curl -X POST http://127.0.0.1:5000/api/influencers/ \
   -H "Content-Type: application/json" \
   -d '{
     "username": "influencer123",
     "full_name": "John Doe",
     "bio": "Lifestyle influencer.",
     "profile_url": "https://www.tiktok.com/@influencer123",
     "followers_count": 50000
   }'
   ```

3. **Alternatively, Run the Test Script:**
   Modify and execute `test.py` to automate endpoint testing.

---

## **Importing Data**

1. **Download Data Files:**  
   - Files needed: `influencer`, `hashtag`, `influencer_hashtag`.  
   - Place the files in the `/data` directory.

2. **Run the Ingestion Scripts:**
   ```bash
   python -m ingestion.hashtag_ingestion
   python -m ingestion.influencer_ingestion
   python -m ingestion.influencer_hashtag_ingestion
   ```

---
