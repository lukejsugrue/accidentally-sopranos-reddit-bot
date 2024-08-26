import psycopg2
from psycopg2.extras import execute_values
import hashlib

# output = pdfminer.high_level.extract_text("raw_scripts/48624984-The-Sopranos-1x05-College.pdf")
# "raw_scripts/48624984-The-Sopranos-1x05-College.txt"
def build_quote_table_from_script_file(filename):
    output = []

    with open(filename, 'r+') as rf:
        lines = rf.readlines()[1::2]  # use slice to avoid escape character
        line_iter= iter(lines)
        for line in line_iter:
            if (any(map(line.__contains__, ["TONY", "CARMELLA", "MEADOW", "IRINA", "ANTHONY JR", "CHRISTOPHER"]))):
                output.append(str(next(line_iter)).strip())

    print(output)

# Function to compute hash of a string
def compute_hash(text):
    return hashlib.md5(text.encode('utf-8')).hexdigest()

# put all of the db functions into their own class
def creat_db_tables():
    # Connect to PostgreSQL
    conn = psycopg2.connect("CONNECTION_STRING")
    cur = conn.cursor()
    # Create tables
    cur.execute("""
        CREATE TABLE IF NOT EXISTS RedditComments (
            id SERIAL PRIMARY KEY,
            content TEXT,
            content_hash TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS SopranosQuotes (
            id SERIAL PRIMARY KEY,
            character TEXT,
            content TEXT,
            content_hash TEXT
        )
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_reddit_hash ON reddit_comments(content_hash)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sentences_hash ON sentences(content_hash)")
