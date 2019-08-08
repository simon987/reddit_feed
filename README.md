# reddit_feed

Fetches comments & submissions from reddit and publishes
 serialised JSON to RabbitMQ for real-time ingest.

Can read up to 100 items per second, as per the API limits 
(60 requests per minute, 100 items per request).
