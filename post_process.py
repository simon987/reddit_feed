import re

HTML_LINK_RE = re.compile(r'href="([^"]+)"')
LINK_RE = re.compile(r'\[.*\]\(([^)]+)\)')
INTERNAL_RE = re.compile(r"^https?://(reddit.com|redd.it|old.reddit.com|www.reddit.com|np.reddit.com)/(.*)")


def post_process(thing):
    thing["v"] = 1.0

    thing["urls"] = []

    if "body_html" in thing and thing["body_html"]:
        thing["urls"].extend(get_links_from_body_html(thing["body_html"]))
    elif "body" in thing and thing["body"]:
        thing["urls"].extend(get_links_from_body(thing["body"]))

    if "selftext_html" in thing and thing["selftext_html"]:
        thing["urls"].extend(get_links_from_body_html(thing["selftext_html"]))
    elif "selftext" in thing and thing["selftext"]:
        thing["urls"].extend(get_links_from_body(thing["selftext"]))

    if thing["urls"]:
        print(thing["urls"])

    if "url" in thing and thing["url"] and is_external(thing["url"]):
        thing["urls"].append(thing["url"])

    return thing


def get_links_from_body_html(body):
    result = set()

    for match in HTML_LINK_RE.finditer(body):
        url = match.group(1)
        if is_external(url):
            result.add(url)

    return list(result)


def get_links_from_body(body):
    result = set()

    body = body.replace("\\)", "&#x28;")
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

