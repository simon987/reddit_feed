from influxdb import InfluxDBClient

client = InfluxDBClient("localhost", 8086, "root", "root", "reddit_feed")


def init():
    db_exists = False
    for db in client.get_list_database():
        if db["name"] == "reddit_feed":
            db_exists = True
            break

    if not db_exists:
        client.create_database("reddit_feed")


def log(event):
    client.write_points(event)


