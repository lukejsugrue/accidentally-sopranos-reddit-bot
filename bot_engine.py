import os
import praw
import pdb
import re

reddit = praw.Reddit('sopranos_bot')

subreddit = reddit.subreddit("mildlyinfuriating")

if not os.path.isfile("soprano_posts.txt"):
    found_quotes = []

else:
    # Read the file into a list and remove any empty values
    with open("soprano_posts.txt", "r") as f:
        found_quotes = f.read()
        found_quotes = found_quotes.split("\n")
        found_quotes = list(filter(None, found_quotes))

for submission in subreddit.hot(limit=50):
    print(submission.title)

    if submission.id not in found_quotes:
