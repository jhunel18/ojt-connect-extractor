from pymongo import MongoClient
import psycopg2
from psycopg2.extras import Json, execute_values
import os

# === Configuration ===
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
PG_DSN    = os.getenv("PG_DSN", "host=localhost dbname=mydb user=myuser password=mypassword")
BATCH     = 1000

# === Connections ===
mongo = MongoClient(MONGO_URI)
db = mongo.ojtconnect
users_coll = db.users
profiles_coll = db.company_profiles

pg = psycopg2.connect(PG_DSN)
cur = pg.cursor()

# Step 1: Migrate users (all types) into users_access
user_batch = []
for user in users_coll.find():
    mongo_id    = str(user.get('_id'))
    name        = user.get('name')
    address     = user.get('address')
    email       = user.get('email')
    password    = user.get('password')
    user_type   = user.get('type')
    is_verify   = user.get('is_verify')
    date_created= user.get('date_created')

    user_batch.append((mongo_id, name, address, email, password, user_type, is_verify, date_created))
    if len(user_batch) >= BATCH:
        execute_values(cur,
            """
            INSERT INTO users_access (mongo_id, name, address, email, password, user_type, is_verify, date_created)
            VALUES %s ON CONFLICT (mongo_id) DO NOTHING
            """,
            user_batch
        )
        pg.commit()
        user_batch = []

if user_batch:
    execute_values(cur,
        """
        INSERT INTO users_access (mongo_id, name, address, email, password, user_type, is_verify, date_created)
        VALUES %s ON CONFLICT (mongo_id) DO NOTHING
        """,
        user_batch
    )
    pg.commit()

# Step 2: Migrate company profiles for users with type='company'
for profile in profiles_coll.find():
    company_mongo_id = str(profile.get('company_id'))

    cur.execute("SELECT user_id FROM users_access WHERE mongo_id = %s", (company_mongo_id,))
    row = cur.fetchone()
    if not row:
        print(f"Warning: No users_access found for company_id {company_mongo_id}")
        continue

    user_id = row[0]
    avatar       = profile.get('avatar')
    about_me     = profile.get('about_me')
    links        = profile.get('links', {})
    created_at   = profile.get('created_at')

    cur.execute("""
        INSERT INTO company_profile (user_id, avatar, about_me, links, created_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (user_id) DO NOTHING
        """,
        (user_id, avatar, about_me, Json(links), created_at)
    )
    pg.commit()

# Cleanup
cur.close()
pg.close()
mongo.close()

print("Migration completed successfully!")
