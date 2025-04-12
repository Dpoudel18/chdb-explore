from chdb import session as chs
from pathlib import Path

# Create DB, Table, View in temp session, auto cleanup when session is deleted.
session = chs.Session()
session.query("CREATE DATABASE IF NOT EXISTS db_movie_streaming")
session.query("CREATE TABLE IF NOT EXISTS db_movie_streaming.cleaned_movie_rating (userid int, movieid varchar, rating int) ENGINE = Log;")

# Example raw data includes (in raw_log_rating.parquet file)
# message (column name)
# "2025-03-02T05:45:56,53723,GET /rate/hellyys+1972=3"
# "2025-03-02T05:45:57,65357,GET /rate/ice+age+2002=3"
# "2025-03-02T05:45:59,32729,GET /rate/the+lord+of+the+rings+the+fellowship+of+the+ring+2001=4"

session.query("""
INSERT INTO db_movie_streaming.cleaned_movie_rating (userid, movieid, rating)
SELECT 
    cast(splitByChar(',', IFNULL(message, ''))[2] as int) AS userid,  -- Extract userID
    extract(splitByChar(',', IFNULL(message, ''))[3], '/rate/([^=]+)') AS movieid,  -- Extract movieid
    cast(extract(splitByChar(',', IFNULL(message, ''))[3], '(\\d+)$') as int) AS rating  -- Extract rating
FROM file('raw_log_rating.parquet', 'Parquet')
WHERE message LIKE '%,GET /rate/%';
""")

# Display the first 10 rows of the cleaned table
print(session.query("SELECT * FROM db_movie_streaming.cleaned_movie_rating limit 10", "Pretty"))

result = session.query("SELECT * FROM db_movie_streaming.cleaned_movie_rating", "Parquet")

# Save to Parquet file in the same directory where this code script resides
output_path = Path('cleaned_movie_ratings.parquet')
output_path.write_bytes(result.bytes())