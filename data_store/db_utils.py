import psycopg2
from psycopg2.extras import execute_values
import hashlib
from dotenv import load_dotenv
import os

class DbUtils:
    # shared across module
    load_dotenv()

    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')

    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password
    )
    cur = conn.cursor()

    # method takes script file, finds quotes, adds them to the db
    def build_quote_table_from_script_file(self, filename):
        output = []
        characters = ["TONY", "CARMELLA", "MEADOW", "IRINA", "ANTHONY JR", "CHRISTOPHER", "PAULIE", "SIL"]

        with open(filename, 'r+') as rf:
            lines = rf.readlines()[1::2]  # use slice to avoid escape character
            line_iter= iter(lines)
            for line in line_iter:
                character = next((char for char in characters if char in line), None)
                if character:
                    quote = str(next(line_iter)).strip()
                    output.append( (character, quote, compute_hash(quote)) )

        # create db tables
        self.create_db_tables()

        # insert data into quotes table
        self.populate_sopranos_table(output)

    # Create tables
    def create_db_tables(self):
        self.cur.execute("""
            CREATE TABLE RedditComments (
                id TEXT PRIMARY KEY,
                comment TEXT,
                comment_hash TEXT,
                processed BOOLEAN DEFAULT FALSE
            )
        """)

        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS SopranosQuotes (
                id SERIAL PRIMARY KEY,
                character_name TEXT,
                quote TEXT,
                quote_hash TEXT
            )
        """)

        self.cur.execute("CREATE INDEX IF NOT EXISTS idx_reddit_hash ON RedditComments(comment_hash)")
        self.cur.execute("CREATE INDEX IF NOT EXISTS idx_sopranos_hash ON SopranosQuotes(quote_hash)")
    
    # params: List<(character, quote, quote_hash)>
    def populate_sopranos_table(self, data):
        execute_values(self.cur, f"INSERT INTO SopranosQuotes (character_name, quote, quote_hash) VALUES %s", data)
        self.conn.commit()
    
    def store_comments(self, comments):
        comments = []
        print(comments)
        for comment in comments:
            comments.append((
                comment.id,
                comment.body,
                self.compute_hash(comment.body),
                False
            ))
        
        execute_values(self.cur, 
            "INSERT INTO reddit_comments (id, content, content_hash, processed) VALUES %s ON CONFLICT (id) DO NOTHING",
            comments
        )
        self.conn.commit()
        print(f"Stored {len(comments)} comments")

    def find_matches(self):
        self.cur.execute("""
            SELECT rc.id, rc.content, sq.quote, sq.character_name 
            FROM reddit_comments rc
            JOIN SopranosQuotes sq ON rc.content_hash = sq.quote_hash
            WHERE rc.processed = FALSE
        """)
        return self.cur.fetchall()


# helper function to compute hash of a string
@staticmethod
def compute_hash(text):
    return hashlib.md5(text.encode('utf-8')).hexdigest()

if __name__ == "__main__":
    # some basic tests
    db_acts = DbUtils()
    db_acts.build_quote_table_from_script_file("raw_scripts/48624984-The-Sopranos-1x05-College.txt")