"""Modified version of praw's rate limiter that can reliably use all of the 60 allowed requests per second."""
import time


class GoodRateLimiter(object):

    def __init__(self):
        self.remaining = None
        self.next_request_timestamp = None
        self.reset_timestamp = None
        self.used = None

    def call(self, request_function, set_header_callback, *args, **kwargs):
        self.delay()
        kwargs['headers'] = set_header_callback()
        response = request_function(*args, **kwargs)
        self.update(response.headers)
        return response

    def delay(self):
        if self.next_request_timestamp is None:
            return
        sleep_seconds = self.next_request_timestamp - time.time()
        if sleep_seconds <= 0:
            return
        time.sleep(sleep_seconds)

    def update(self, response_headers):
        if 'x-ratelimit-remaining' not in response_headers:
            if self.remaining is not None:
                self.remaining -= 1
                self.used += 1
            return

        now = time.time()
        prev_remaining = self.remaining

        seconds_to_reset = int(response_headers['x-ratelimit-reset'])
        self.remaining = float(response_headers['x-ratelimit-remaining'])
        self.used = int(response_headers['x-ratelimit-used'])
        self.reset_timestamp = now + seconds_to_reset

        if self.remaining <= 0:
            self.next_request_timestamp = self.reset_timestamp
            return

        if prev_remaining is not None and prev_remaining > self.remaining:
            estimated_clients = prev_remaining - self.remaining
        else:
            estimated_clients = 1.0

        self.next_request_timestamp = min(
            self.reset_timestamp,
            now + (estimated_clients * seconds_to_reset / self.remaining)
            if (seconds_to_reset > self.remaining) and self.remaining > 5 else 0
        )
