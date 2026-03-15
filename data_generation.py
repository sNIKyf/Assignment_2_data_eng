import mysql.connector
import json
import os
import random
import time
from datetime import datetime, timedelta

DB_CONFIG = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "MySQL_Student123",
    "database": "test_db",
    "port": 3306
}

JSON_OUTPUT_DIR = './data/telephony_data/' 
NUM_EMPLOYEES = 50

FIRST_NAMES = ["Alex", "Jordan", "Taylor", "Morgan", "Casey"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones"]
TEAMS = ["Billing", "Tech Support", "Account Management"]
STATUSES = ["Completed", "Dropped", "Transferred"]
DIRECTIONS = ["Inbound", "Outbound"]

def setup_database(cursor):
    print("Creating tables...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS employees (
            employee_id INT AUTO_INCREMENT PRIMARY KEY,
            full_name VARCHAR(255) NOT NULL,
            team VARCHAR(100),
            hire_date DATE
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS calls (
            call_id INT AUTO_INCREMENT PRIMARY KEY,
            employee_id INT,
            call_time DATETIME,
            phone VARCHAR(20),
            direction VARCHAR(20),
            status VARCHAR(50),
            FOREIGN KEY (employee_id) REFERENCES employees(employee_id)
        )
    """)

def generate_employees(cursor):
    cursor.execute("SELECT COUNT(*) FROM employees")
    if cursor.fetchone()[0] > 0:
        return
    print(f"Generating {NUM_EMPLOYEES} employees...")
    insert_query = "INSERT INTO employees (full_name, team, hire_date) VALUES (%s, %s, %s)"
    employees = [
        (f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}", 
         random.choice(TEAMS), 
         (datetime.now() - timedelta(days=random.randint(0, 1000))).strftime('%Y-%m-%d'))
        for _ in range(NUM_EMPLOYEES)
    ]
    cursor.executemany(insert_query, employees)

def generate_continuous_calls(cursor, conn):
    cursor.execute("SELECT employee_id FROM employees")
    employee_ids = [row[0] for row in cursor.fetchall()]
    
    insert_query = "INSERT INTO calls (employee_id, call_time, phone, direction, status) VALUES (%s, %s, %s, %s, %s)"
    print(f"Generating live data. JSONs saving to: {JSON_OUTPUT_DIR}. Press Ctrl+C to stop.")
    os.makedirs(JSON_OUTPUT_DIR, exist_ok=True)
    
    try:
        while True:
            calls_this_batch = random.randint(1, 3)
            for _ in range(calls_this_batch):
                emp_id = random.choice(employee_ids)
                call_time = datetime.now()
                status = random.choice(STATUSES)
                
                # Insert to MySQL
                cursor.execute(insert_query, (emp_id, call_time.strftime('%Y-%m-%d %H:%M:%S'), "+1-555-0000", random.choice(DIRECTIONS), status))
                call_id = cursor.lastrowid
                
                # Generate JSON mock file
                json_data = {
                    "call_id": call_id,
                    "duration_sec": random.randint(30, 3600),
                    "short_description": "Mock LLM Summary" if status != "Dropped" else "Call dropped."
                }
                with open(os.path.join(JSON_OUTPUT_DIR, f"{call_id}.json"), 'w') as f:
                    json.dump(json_data, f)
            
            conn.commit()
            print(f"Generated {calls_this_batch} calls. Sleeping 10s...")
            time.sleep(10)
    except KeyboardInterrupt:
        print("\nStopped.")

def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    setup_database(cursor)
    generate_employees(cursor)
    conn.commit()
    generate_continuous_calls(cursor, conn)

if __name__ == "__main__":
    main()
