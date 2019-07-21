import asyncio
import cbpro
import queue
from mode import Service
from bc_monit.faust_app import app

cb_topic = app.topic("coinbase_btc", key_type=str, value_type=str)


class WebsocketClient(cbpro.WebsocketClient):
    def on_open(self):
        self.q = queue.Queue()
        self.url = "wss://ws-feed.pro.coinbase.com/"
        self.products = ["BTC-USD"]
        print("Starting coinbase listener!")

    def on_message(self, msg):
        self.q.put((msg, cb_topic.send(value=msg, value_serializer="json")))

    def on_close(self):
        print("-- Goodbye! --")


@app.service
class MyService(Service):
    async def on_start(self):
        print("Coinbase Service Starting")
        self.ws_client = WebsocketClient()
        self.ws_client.start()

    async def on_stop(self):
        print("Coinbase Service Stopping")

    @Service.task
    async def _background_task(self):
        # note all sends need to be awaited on
        while not self.should_stop:
            while self.ws_client.q.qsize() > 0:
                (data, faust_reply) = self.ws_client.q.get()
                try:
                    kafka_reply = await faust_reply
                    # its probably good to change the fuast timeout along with this
                    await asyncio.wait_for(kafka_reply, timeout=10)
                except Exception as e:
                    # We can bail out and write to an alt data sync here if we want
                    # It is probably best to empty the entire q without await though
                    from concurrent.futures._base import TimeoutError

                    if isinstance(e, TimeoutError):
                        print("Timed out connecting to kafka")
                    print(e)

            await self.sleep(0.1)
