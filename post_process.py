import re

LINK_RE = re.compile(r'href="([^"]+)"')
INTERNAL_RE = re.compile(r"^https?://(reddit.com|redd.it|old.reddit.com|www.reddit.com|np.reddit.com)/(.*)")


def post_process(thing):
    thing["v"] = 1.0

    thing["urls"] = []
    if "body_html" in thing and thing["body_html"]:
        thing["urls"].extend(get_links_from_body(thing["body_html"]))
    elif "selftext_html" in thing and thing["selftext_html"]:
        thing["urls"].extend(get_links_from_body(thing["selftext_html"]))

    if "url" in thing and thing["url"] and is_external(thing["url"]):
        thing["urls"].append(thing["url"])

    return thing


def get_links_from_body(body):
    result = set()

    for match in LINK_RE.finditer(body):
        url = match.group(1)
        if is_external(url):
            result.add(url)

    return list(result)


def is_external(url):

    if INTERNAL_RE.match(url):
        return False

    if "message/compose" in url:
        return False

    if url.startswith(("/", "#", "</")):
        return False

    return True

