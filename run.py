#!/bin/env python

import datetime
import json
import logging
import sys
import threading
import time
import traceback
from datetime import datetime, timedelta
from itertools import islice
from logging import FileHandler, StreamHandler
from queue import Queue

import pika
import praw
from praw.endpoints import API_PATH
from praw.models import Comment

import monitoring
from post_process import post_process
from rate_limiter import GoodRateLimiter
from util import update_cursor, read_cursor, reddit_ids

reddit = praw.Reddit('archivist-bot')

# Fix praw's default rate limiter
reddit._core._rate_limiter = GoodRateLimiter()

logger = logging.getLogger("default")
logger.setLevel(logging.DEBUG)

rabbit = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
reddit_channel = rabbit.channel()
reddit_channel.exchange_declare(exchange="reddit", exchange_type="topic")


REALTIME_DELAY = timedelta(seconds=60)
MONITORING = True

formatter = logging.Formatter('%(asctime)s %(levelname)-5s %(message)s')
file_handler = FileHandler("reddit_feed.log")
file_handler.setFormatter(formatter)
for h in logger.handlers:
    logger.removeHandler(h)
logger.addHandler(file_handler)
logger.addHandler(StreamHandler(sys.stdout))


def serialize(thing):
    if isinstance(thing, Comment):
        return {
            "author": str(thing.author),
            "author_flair_text": thing.author_flair_text,
            "body": thing.body,
            "selftext_html": thing.body_html if hasattr(thing, "body_html") else None,
            "controversiality": thing.controversiality,
            "created": int(thing.created),
            "created_utc": int(thing.created_utc),
            "downs": thing.downs if hasattr(thing, "downs") else 0,
            "id": thing.id,
            "link_id": thing.link_id,
            "name": thing.name,
            "parent_id": thing.parent_id,
            "permalink": thing.permalink,
            "score": thing.score,
            "subreddit": str(thing.subreddit),
            "subreddit_id": thing.subreddit_id,
            "subreddit_type": thing.subreddit_type,
            "ups": thing.ups,
        }
    else:
        return {
            "author": str(thing.author),
            "author_flair_text": thing.author_flair_text,
            "created": int(thing.created),
            "created_utc": int(thing.created_utc),
            "domain": thing.domain,
            "id": thing.id,
            "is_crosspostable": thing.is_crosspostable,
            "is_meta": thing.is_meta,
            "is_original_content": thing.is_original_content,
            "is_reddit_media_domain": thing.is_reddit_media_domain,
            "is_robot_indexable": thing.is_robot_indexable,
            "is_self": thing.is_self,
            "is_video": thing.is_video,
            "link_flair_text": thing.link_flair_text if hasattr(thing, "link_flair_text") else None,
            "locked": thing.locked,
            "num_comments": thing.num_comments,
            "num_crossposts": thing.num_crossposts,
            "over_18": thing.over_18,
            "parent_whitelist_status": thing.parent_whitelist_status,
            "permalink": thing.permalink,
            "pinned": thing.pinned,
            "score": thing.score,
            "selftext": thing.selftext if hasattr(thing, "selftext") else None,
            "selftext_html": thing.selftext_html if hasattr(thing, "selftext_html") else None,
            "spoiler": thing.spoiler,
            "stickied": thing.stickied,
            "subreddit": str(thing.subreddit),
            "subreddit_id": thing.subreddit_id,
            "subreddit_subscribers": thing.subreddit_subscribers,
            "subreddit_type": thing.subreddit_type,
            "title": thing.title,
            "ups": thing.ups if hasattr(thing, "ups") else 0,
            "downs": thing.downs if hasattr(thing, "downs") else 0,
            "url": thing.url,
        }


def publish(thing):
    thing_type = type(thing).__name__.lower()
    j = serialize(thing)
    post_process(j)

    reddit_channel.basic_publish(
        exchange='reddit',
        routing_key="%s.%s" % (thing_type, str(thing.subreddit).lower()),
        body=json.dumps(j)
    )


def publish_worker(q: Queue):
    logger.info("Started publish thread")
    while True:
        try:
            thing = q.get()
            publish(thing)

        except Exception as e:
            logger.error(str(e) + ": " + traceback.format_exc())
        finally:
            q.task_done()


def mon_worker(q: Queue):
    logger.info("Started monitoring thread")
    while True:
        try:
            event = q.get()
            monitoring.log(event)

        except Exception as e:
            logger.error(str(e) + ": " + traceback.format_exc())
        finally:
            q.task_done()


def stream_thing(prefix, publish_queue, mon_queue=None, start_id=None):
    if start_id is None:
        start_id = read_cursor(prefix)

    logger.info("Starting stream for %s at cursor %s" % (prefix, start_id))

    iterable = reddit_ids(prefix, start_id)

    while True:
        chunk = list(islice(iterable, 100))

        params = {"id": ",".join(chunk)}
        results = reddit.get(API_PATH["info"], params=params)
        post_time = datetime.utcfromtimestamp(results[0].created_utc)
        now = datetime.utcnow()
        distance = now - post_time

        logger.debug("[+%d] (%s) We are %s away from realtime (%s)" %
                     (len(results), prefix, distance, datetime.fromtimestamp(results[0].created_utc)))
        if distance < REALTIME_DELAY:
            sleep_time = (REALTIME_DELAY - distance).total_seconds()
            logger.info("Sleeping %s seconds" % sleep_time)
            time.sleep(sleep_time)

        update_cursor(results[0].id, prefix)

        for result in results:
            publish_queue.put(result)

        if MONITORING:
            mon_queue.put([{
                "measurement": "reddit",
                "time": str(datetime.utcnow()),
                "tags": {
                    "item_type": prefix,
                },
                "fields": {
                    "item_count": len(results),
                    "distance": distance.total_seconds()
                }
            }])


if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("You must specify RabbitMQ host!")
        quit(1)

    logger.info("Starting app @%s" % sys.argv[1])

    monitoring.init()
    pub_queue = Queue()
    while True:
        try:
            publish_thread = threading.Thread(target=publish_worker, args=(pub_queue,))
            if MONITORING:
                monitoring_queue = Queue()
                log_thread = threading.Thread(target=mon_worker, args=(monitoring_queue,))
                log_thread.start()

                comment_thread = threading.Thread(target=stream_thing, args=("t1_", pub_queue, monitoring_queue))
                post_thread = threading.Thread(target=stream_thing, args=("t3_", pub_queue, monitoring_queue))
            else:
                comment_thread = threading.Thread(target=stream_thing, args=("t1_", pub_queue))
                post_thread = threading.Thread(target=stream_thing, args=("t3_", pub_queue))

            comment_thread.start()
            post_thread.start()
            publish_thread.start()
        except Exception as e:
            logger.error(str(e) + ": " + traceback.format_exc())

        while True:
            time.sleep(10)
