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
profiles_coll = db.company_profiles

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
CREATE TABLE IF NOT EXISTS users_access (
    user_id SERIAL PRIMARY KEY,
    mongo_id TEXT UNIQUE,
    name TEXT,
    address TEXT,
    email TEXT,
    password TEXT,
    user_type TEXT,
    is_verify BOOLEAN,
    date_created TIMESTAMP
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS company_profile (
    profile_id SERIAL PRIMARY KEY,
    user_id INTEGER UNIQUE REFERENCES users_access(user_id),
    name VARCHAR(255),
    address TEXT,
    avatar TEXT,
    about_me TEXT,
    fb_link TEXT,
    instagram_link TEXT,
    linkedin_link TEXT,
    portfolio_link TEXT,
    created_at TIMESTAMP
);
""")
pg.commit()  # don’t forget to commit table creation :contentReference[oaicite:0]{index=0}

# === Step 1: Migrate users ===
user_batch = []
for user in users_coll.find():
    user_batch.append((
        str(user.get('_id')),
        user.get('name'),
        user.get('address'),
        user.get('email'),
        user.get('password'),
        user.get('type'),
        user.get('is_verify'),
        user.get('date_created'),
    ))
    if len(user_batch) >= BATCH:
        execute_values(cur,
            """
            INSERT INTO users_access (mongo_id, name, address, email, password, user_type, is_verify, date_created)
            VALUES %s ON CONFLICT (mongo_id) DO NOTHING;
            """,
            user_batch
        )
        pg.commit()
        user_batch.clear()

if user_batch:
    execute_values(cur,
        """
        INSERT INTO users_access (mongo_id, name, address, email, password, user_type, is_verify, date_created)
        VALUES %s ON CONFLICT (mongo_id) DO NOTHING;
        """,
        user_batch
    )
    pg.commit()

# === Step 2: Migrate company profiles ===
for profile in profiles_coll.find():
    company_mongo_id = str(profile.get('company_id'))
    cur.execute("SELECT user_id, name, address FROM users_access WHERE user_type = 'company' AND mongo_id = %s;", (company_mongo_id,))
    row = cur.fetchone()

    if not row:
        print(f"Warning: No user found for company_id {company_mongo_id}")
        continue

    links = profile.get('links', {})
    cur.execute(
        """
        INSERT INTO company_profile (user_id, name, address, avatar, about_me, fb_link, instagram_link, linkedin_link, portfolio_link, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (user_id) DO NOTHING;
        """,
        (
            row[0],
            row[1],
            row[2],
            profile.get('avatar'),
            profile.get('about_me'),
            links.get('facebook'),
            links.get('linkedin'),
            links.get('instagram'),
            links.get('portfolio'),
            profile.get('created_at')
        )
    )
    pg.commit()

# === Cleanup ===
cur.close()
pg.close()
mongo.close()
print("Tables created and migration completed successfully!")
