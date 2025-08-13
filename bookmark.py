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
bookmark_coll = db.bookmarks

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
CREATE TABLE IF NOT EXISTS users_access_bookmark (
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
CREATE TABLE IF NOT EXISTS bookmarks (
    bookmark_id SERIAL PRIMARY KEY,
    job_id INTEGER REFERENCES job_post(job_id),
    student_id INTEGER REFERENCES users_access_bookmark(user_id),
    bookmark_date TIMESTAMP
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
            INSERT INTO users_access_bookmark (mongo_id, email, password, user_type, is_verify, date_created)
            VALUES %s ON CONFLICT (mongo_id) DO NOTHING;
            """,
            user_batch
        )
        pg.commit()
        user_batch.clear()

if user_batch:
    execute_values(cur,
        """
        INSERT INTO users_access_bookmark (mongo_id, email, password, user_type, is_verify, date_created)
        VALUES %s ON CONFLICT (mongo_id) DO NOTHING;
        """,
        user_batch
    )
    pg.commit()

for profile in bookmark_coll.find():
    student_mongo_id = str(profile.get('student_id'))
    job_mongo_id = str(profile.get('job_id'))
    cur.execute("SELECT user_id FROM users_access_bookmark WHERE user_type = 'student' AND mongo_id = %s;", (student_mongo_id,))
    row = cur.fetchone()

    if not row:
        print(f"Warning: No user found for student_id {student_mongo_id}")
        continue

    cur.execute("SELECT job_id FROM job_post WHERE mongo_id = %s;", (job_mongo_id,))
    job_row = cur.fetchone()
    if not job_row:
        print(f"Warning: No job found for job_id {job_mongo_id}")
        continue
    
    cur.execute(
        """
        INSERT INTO bookmarks (job_id, student_id, bookmark_date)
        VALUES (%s, %s, %s);
        """,
        (
            job_row[0],
            row[0],
            profile.get('bookmark_date'),
        )
    )
    pg.commit()

# === Cleanup ===
cur.close()
pg.close()
mongo.close()
print("Tables created and migration completed successfully!")
