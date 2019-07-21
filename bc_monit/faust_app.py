import faust

app = faust.App(
    "proj",
    version=1,
    broker="kafka://192.168.99.103:9092",
    store="rocksdb://",
    autodiscover=True,
    origin="bc_monit",  # imported name for this project (import proj -> "proj")
)


def main():
    app.main()
