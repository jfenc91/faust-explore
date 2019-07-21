from aiokafka import AIOKafkaProducer
import asyncio
import cbpro
from avro.io import DatumWriter, BinaryEncoder
from avro.schema import Parse
import io
import queue


class WebsocketClient(cbpro.WebsocketClient):
    def __init__(self):
        super().__init__()
        self.url = "wss://ws-feed.pro.coinbase.com/"
        self.products = ["BTC-USD"]
        print("Starting coinbase listener!")
        self.q = queue.Queue()
        # Path to user.avsc avro schema

        loop = asyncio.get_event_loop()
        self.producer = AIOKafkaProducer(
            loop=loop, bootstrap_servers="192.168.99.103:9092"
        )

        SCHEMA_PATH = "./avro/transaction.avsc.json"
        with open(SCHEMA_PATH) as f:
            avro_schema = Parse(f.read())

        self.writer = DatumWriter(avro_schema)
        self.bytes_writer = io.BytesIO()
        self.encoder = BinaryEncoder(self.bytes_writer)

    def on_open(self):
        print("opening")

    def on_message(self, msg):
        if "price" in msg and "type" in msg:
            self.writer.write(msg, self.encoder)
            raw_bytes = self.bytes_writer.getvalue()
            self.bytes_writer.seek(0)
            self.q.put(
                (raw_bytes, self.producer.send_and_wait("coinbase_btc", raw_bytes))
            )

    def on_close(self):
        print("-- Goodbye! --")


class CoinbaseTransactionKafkaProducer:
    async def start(self):
        ws_client = WebsocketClient()
        await ws_client.producer.start()
        ws_client.start()

        while True:
            (data, kafka_reply) = ws_client.q.get()
            try:
                await asyncio.wait_for(kafka_reply, timeout=100)
            except Exception as e:
                from concurrent.futures._base import TimeoutError

                if isinstance(e, TimeoutError):
                    print("Timed out connecting to kafka")
                print(e)
                if hasattr(e, "message"):
                    print(e.message)
                # short circuit the await for large queue sizes after failure
                while ws_client.q.qsize() > 100:
                    (data, kafka_reply) = ws_client.q.get()
                    # print("Failed to write: {}".format(data))
                print("Failed to write: {}".format(data))


def main():
    loop = asyncio.get_event_loop()
    cb_producer = CoinbaseTransactionKafkaProducer()
    loop.run_until_complete(cb_producer.start())


if __name__ == "__main__":
    main()
