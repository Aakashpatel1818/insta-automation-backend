"""
Run this script to promote your user to superadmin directly in MongoDB.
Usage: python make_superadmin.py your@email.com
"""
import asyncio
import sys
from motor.motor_asyncio import AsyncIOMotorClient

MONGO_URL = "mongodb://localhost:27017"
DB_NAME   = "insta_db"

async def main(email: str):
    client = AsyncIOMotorClient(MONGO_URL)
    db = client[DB_NAME]

    # Show all users
    print("\n--- All users in DB ---")
    users = await db["users"].find({}, {"email": 1, "username": 1, "role": 1}).to_list(100)
    for u in users:
        print(f"  {u.get('email')} | role: {u.get('role', 'user')} | username: {u.get('username')}")

    if not email:
        print("\nProvide your email as argument: python make_superadmin.py your@email.com")
        return

    # Reset ALL roles to user first (clean slate)
    await db["users"].update_many({}, {"$set": {"role": "user"}})
    print(f"\n✓ Reset all roles to 'user'")

    # Promote the target email
    result = await db["users"].update_one(
        {"email": email.lower().strip()},
        {"$set": {"role": "superadmin"}}
    )

    if result.matched_count == 0:
        print(f"\n✗ No user found with email: {email}")
        print("  Check the email list above and try again.")
    else:
        print(f"✓ Promoted {email} to superadmin!")
        print("\nNext: logout and login again on the frontend.")

    client.close()

if __name__ == "__main__":
    email = sys.argv[1] if len(sys.argv) > 1 else ""
    asyncio.run(main(email))
