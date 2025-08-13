from pymongo import MongoClient
import psycopg2
from psycopg2.extras import Json, execute_values
import os
from dotenv import load_dotenv
load_dotenv()

# === Configuration ===
# MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
# PG_DSN    = os.getenv("PG_DSN", "host=localhost dbname=mydb user=myuser password=mypassword")
BATCH     = 1066

# === Connect ===
mongo = MongoClient(os.getenv("MONGO_URI"))
db = mongo.ojtconnect
users_coll = db.users
job_coll = db.job_posts

pg = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),    
        database=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        sslmode = os.getenv("PG_SSLMODE")  # Secure SSL connection
    )
cur = pg.cursor()

# === Step 0: Create tables if they don't exist ===
cur.execute("""
CREATE TABLE IF NOT EXISTS users_access_job_post (
    user_id SERIAL PRIMARY KEY,
    mongo_id TEXT UNIQUE,
    email TEXT,
    password TEXT,
    user_type TEXT,
    is_verify BOOLEAN,
    date_created TIMESTAMP
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS job_post (
    job_id SERIAL PRIMARY KEY,
    mongo_id TEXT UNIQUE,
    company_id INTEGER UNIQUE REFERENCES users_access_job_post(user_id),
    position VARCHAR(100),
    category VARCHAR(40),
    work_setup VARCHAR(20),
    description TEXT,
    duration TEXT,
    hours_per_week TEXT,
    status VARCHAR(15),
    is_available BOOLEAN,
    date_posted TIMESTAMP
);
""")
pg.commit()  # don’t forget to commit table creation :contentReference[oaicite:0]{index=0}

# === Step 1: Migrate users ===
user_batch = []
for user in users_coll.find():
    user_batch.append((
        str(user.get('_id')),
        user.get('email'),
        user.get('password'),
        user.get('type'),
        user.get('is_verify'),
        user.get('date_created'),
    ))
    if len(user_batch) >= BATCH:
        execute_values(cur,
            """
            INSERT INTO users_access_job_post (mongo_id, email, password, user_type, is_verify, date_created)
            VALUES %s ON CONFLICT (mongo_id) DO NOTHING;
            """,
            user_batch
        )
        pg.commit()
        user_batch.clear()

if user_batch:
    execute_values(cur,
        """
        INSERT INTO users_access_job_post (mongo_id, email, password, user_type, is_verify, date_created)
        VALUES %s ON CONFLICT (mongo_id) DO NOTHING;
        """,
        user_batch
    )
    pg.commit()

# === Step 2: Migrate company profiles ===
for profile in job_coll.find():
    company_mongo_id = str(profile.get('company_id'))
    cur.execute("SELECT user_id FROM users_access_job_post WHERE user_type = 'company' AND mongo_id = %s;", (company_mongo_id,))
    row = cur.fetchone()

    if not row:
        print(f"Warning: No user found for company_id {company_mongo_id}")
        continue
    cur.execute(
        """
        INSERT INTO job_post (mongo_id, company_id, position, category, work_setup, description, duration, hours_per_week, status, is_available, date_posted)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (company_id) DO NOTHING;
        """,
        (
            str(profile.get('_id')),
            row[0],
            profile.get('position'),
            profile.get('category'),
            profile.get('work_setup'),
            profile.get('description'),
            profile.get('duration'),
            profile.get('hours_per_week'),
            profile.get('status'),
            profile.get('is_available'),
            profile.get('date_posted')
        )
    )
    pg.commit()

# === Cleanup ===
cur.close()
pg.close()
mongo.close()
print("Tables created and migration completed successfully!")
