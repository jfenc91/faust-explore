from bc_monit.faust_app import app
import faust
from datetime import datetime


class Transaction(faust.Record):
    product_id: str
    sequence: int
    side: str
    time: str
    type: str
    price: str = None
    client_oid: str = None
    make_order_id: str = None
    taker_order_id: str = None
    order_id: str = None
    order_type: str = None
    reason: str = None
    remaining_size: str = None
    size: str = None
    trade_id: int = None


class TypeCount(faust.Record):
    type: str
    count: int
    window: float


cb_btc_topic = app.topic("coinbase_btc", value_type=Transaction)
type_count_channel = app.channel(value_type=TypeCount)
aggregated_counts_channel = app.channel()

count_window = 5
counts = (
    app.Table("event_counts", default=int, partitions=1)
    .tumbling(count_window, expires=count_window, key_index=False)
    .relative_to_now()
)
api_data_table = app.Table("api_data")

last_max = {}

@app.page('/counts/latest')
async def myview(self, request):
    return {}

@app.agent(aggregated_counts_channel)
async def publish_counts(counts):
    async for event in counts:
        print(event)

@app.agent()
async def print_counts(stream):
    async for events in stream.take(10000, within=10):
        max_by_window = {}
        for event in events:
            key = (event.type, (event.window[0], event.window[1]))
            if key not in max_by_window or max_by_window[key] < event.count:
                max_by_window[key] = event.count
        await aggregated_counts_channel.put(max_by_window)


@app.agent(cb_btc_topic)
async def count_events(events):
    async for event in events.group_by(Transaction.type, partitions=1):
        window = counts.table.window
        current_window = window.current(datetime.utcnow().timestamp())
        current = counts[(event.type, current_window)].now()
        new_value = 1 + current
        counts[(event.type, current_window)] = new_value
        await print_counts.send(value=TypeCount(event.type, new_value, current_window))
