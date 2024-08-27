import os
import time
import praw
import pdb
import re

from data_store import DbUtils

def main():
    dbLib = DbUtils()

    reddit = praw.Reddit('sopranos_bot')
    subreddit = reddit.subreddit("applyingtocollege")
    print(subreddit.comments(limit=1000))

    dbLib.store_comments( subreddit.comments(limit=1000) )

    matches = dbLib.find_matches()
    print(matches)
    for comment_id, comment, quote, character in matches:
        comment = reddit.comment(comment_id)
        reply = f'"{quote}" - {character} from The Sopranos'
        comment.reply(reply)
        print(f"Replied to comment: {comment_id}")
            
        time.sleep(60)  # Wait for a minute before the next batch

if __name__ == "__main__":
    main()