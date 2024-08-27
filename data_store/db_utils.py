import psycopg2
from psycopg2.extras import execute_values
import hashlib

class DbUtils:
    # shared across module
    conn = psycopg2.connect("dbname=db_90982ed4_0798_4336_b71a_04c883300001 user=u_90982ed4_0798_4336_b71a_04c883300001 password=MHklQ42D8Z2PNp3438R4TWYF79ET391wABUvxg9Zx3Pl8JZnl184 host=pg.rapidapp.io port=5432")
    cur = conn.cursor()

    # output = pdfminer.high_level.extract_text("raw_scripts/48624984-The-Sopranos-1x05-College.pdf")
    # "raw_scripts/48624984-The-Sopranos-1x05-College.txt"
    def build_quote_table_from_script_file(self, filename):
        output = []

        with open(filename, 'r+') as rf:
            lines = rf.readlines()[1::2]  # use slice to avoid escape character
            line_iter= iter(lines)
            for line in line_iter:
                if (any(map(line.__contains__, ["TONY", "CARMELLA", "MEADOW", "IRINA", "ANTHONY JR", "CHRISTOPHER"]))):
                    quote = str(next(line_iter)).strip()
                    output.append( (str(line).strip(), quote, compute_hash(quote)) )

        print(output)

        # check formatting of the quote data

        # create db tables
        self.create_db_tables()

        # insert data into quotes table
        self.populate_sopranos_table(output)

    # Create tables
    def create_db_tables(self):
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS RedditComments (
                id SERIAL PRIMARY KEY,
                comment TEXT,
                comment_hash TEXT
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


# helper function to compute hash of a string
@staticmethod
def compute_hash(text):
    return hashlib.md5(text.encode('utf-8')).hexdigest()

if __name__ == "__main__":
    # some basic tests
    db_acts = DbUtils()
    db_acts.build_quote_table_from_script_file("raw_scripts/48624984-The-Sopranos-1x05-College.txt")