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
student_coll = db.student_profiles

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
CREATE TABLE IF NOT EXISTS users_access_student (
    user_id SERIAL PRIMARY KEY,
    mongo_id TEXT UNIQUE,
    name TEXT,
    university TEXT,
    course TEXT,
    level TEXT,
    email TEXT,
    password TEXT,
    user_type TEXT,
    is_verify BOOLEAN,
    date_created TIMESTAMP
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS student_profile (
    profile_id SERIAL PRIMARY KEY,
    user_id INTEGER UNIQUE REFERENCES users_access_student(user_id),
    name VARCHAR(255),
    university TEXT,
    course TEXT,
    level TEXT,
    avatar TEXT,
    phone_number TEXT,
    skills JSONB,
    certificate JSONB,
    organization JSONB,
    about_me TEXT,
    fb_link TEXT,
    instagram_link TEXT,
    linkedin_link TEXT,
    portfolio_link TEXT,
    date_created TIMESTAMP,
    date_updated TIMESTAMP
);
""")
pg.commit()

user_batch = []
for user in users_coll.find():
    user_batch.append((
        str(user.get('_id')),
        user.get('name'),
        user.get('university'),
        user.get('course'),
        user.get('level'),
        user.get('email'),
        user.get('password'),
        user.get('type'),
        user.get('is_verify'),
        user.get('date_created'),
    ))
    if len(user_batch) >= BATCH:
        execute_values(cur,
            """
            INSERT INTO users_access_student (mongo_id, name, university, course, level, email, password, user_type, is_verify, date_created)
            VALUES %s ON CONFLICT (mongo_id) DO NOTHING;
            """,
            user_batch
        )
        pg.commit()
        user_batch.clear()

if user_batch:
    execute_values(cur,
        """
        INSERT INTO users_access_student (mongo_id, name,  university, course, level, email, password, user_type, is_verify, date_created)
        VALUES %s ON CONFLICT (mongo_id) DO NOTHING;
        """,
        user_batch
    )
    pg.commit()

for profile in student_coll.find():
    student_mongo_id = str(profile.get('student_id'))
    cur.execute("SELECT user_id, name, university, course, level FROM users_access_student WHERE user_type = 'student' AND mongo_id = %s;", (student_mongo_id,))
    row = cur.fetchone()

    if not row:
        print(f"Warning: No user found for student_profile {student_mongo_id}")
        continue

    links = profile.get('links', {})
    cur.execute(
        """
        INSERT INTO student_profile (user_id, name, university, course, level, avatar, phone_number, skills, certificate, organization, about_me, fb_link, instagram_link, linkedin_link, portfolio_link, date_created, date_updated)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (user_id) DO NOTHING;
        """,
        (
            row[0],
            row[1],
            row[2],
            row[3],
            row[4],
            profile.get('avatar'),
            profile.get('phone_number'),
            Json(profile.get('skills')),
            Json(profile.get('certificates')),
            Json(profile.get('organizations')),
            profile.get('about_me'),
            links.get('facebook'),
            links.get('linkedin'),
            links.get('instagram'),
            links.get('portfolio'),
            profile.get('date_updated'),
            profile.get('date_created')
        )
    )
    pg.commit()

# === Cleanup ===
cur.close()
pg.close()
mongo.close()
print("Tables created and migration completed successfully!")
