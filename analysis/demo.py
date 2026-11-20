"""Run the report on a fresh in-memory database using only the Python standard library."""
from pathlib import Path
import sqlite3

folder = Path(__file__).resolve().parent
with sqlite3.connect(":memory:") as db:
    db.executescript((folder / "mock_reviews.sql").read_text(encoding="utf-8"))
    result = db.execute((folder / "review_ratings.sql").read_text(encoding="utf-8"))
    print("category,reviews,scored,unscored,mean_stars")
    for row in result:
        print(",".join(map(str, row)))
