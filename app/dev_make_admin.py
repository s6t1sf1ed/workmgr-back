# backend/app/dev_make_admin.py
import asyncio
from app.db import db

EMAIL = "vadimka-cha@mail.ru"

async def run():
    r = await db.users.update_one({"email": EMAIL}, {"$set": {"role": "admin"}})
    print("matched:", r.matched_count, "modified:", r.modified_count)

if __name__ == "__main__":
    asyncio.run(run())
