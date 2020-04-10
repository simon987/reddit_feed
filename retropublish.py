# Script to retroactively publish reddit items from pushshift for a specific subreddit
import time
import traceback

import psaw
import sys

from run import publish, logger

if len(sys.argv) != 3:
    print("Usage: ./retropublish.py post|comment subreddit")
    quit(0)

item_type = sys.argv[1]
subreddit = sys.argv[2]

p = psaw.PushshiftAPI()

if item_type == "post":
    gen = p.search_submissions(subreddit=subreddit, limit=100000000)
else:
    gen = p.search_comments(subreddit=subreddit, limit=1000000000)

for item in gen:
    try:
        publish(item)
    except Exception as e:
        logger.error(str(e) + ": " + traceback.format_exc())
