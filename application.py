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
application_coll = db.applications

pg = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),    
        database=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        sslmode = os.getenv("PG_SSLMODE")  # Secure SSL connection
    )
cur = pg.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS application (
    application_id SERIAL PRIMARY KEY,
    job_id INTEGER REFERENCES job_post(job_id),
    student_id INTEGER REFERENCES users_access(user_id),
    message TEXT,
    resume TEXT,
    status VARCHAR(20),
    application_date TIMESTAMP,
    interview_date TIMESTAMP,
    meeting_link TEXT,
    is_accepted BOOLEAN
);
""")
pg.commit()  # don’t forget to commit table creation :contentReference[oaicite:0]{index=0}

for profile in application_coll.find():
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
        INSERT INTO application (job_id, student_id, message, resume, status, application_date, interview_date, meeting_link, is_accepted)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """,
        (
            job_row[0],
            row[0],
            profile.get('message'),
            profile.get('resume'),
            profile.get('status'),
            profile.get('application_date'),
            profile.get('interview_date'),
            profile.get('meeting_link'),
            profile.get('is_accepted')
        )
    )
    pg.commit()

# === Cleanup ===
cur.close()
pg.close()
mongo.close()
print("Tables created and migration completed successfully!")
