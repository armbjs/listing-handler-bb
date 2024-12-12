import datetime
import pytz
import queue
import time
import pathlib
import threading
import json
import re
import os
import decimal
import logging
import math

import dotenv

if __package__ == None or __package__ == '':
    import redis_client
else:
    from . import redis_client

# pybit 라이브러리 임포트 (테스트 코드 기반)
from pybit.unified_trading import HTTP

env_file_path = pathlib.Path(__file__).parent.parent / ".env"
print("env_file_path", env_file_path)
dotenv.load_dotenv(env_file_path, override=True)


class PubSubManager(redis_client.real_redis_client_interface.RealRedisClientInterface):
    def prepare_pubsub(self, message_handler):
        self.pubsub = self.raw_redis_client.pubsub()
        self.is_pubsub_listener_running = False
        self.is_queue_flusher_running = False
        self.message_handler = message_handler
        self.message_queue = queue.Queue()

    def subscribe(self, redis_publish_channel_key_name: str):
        self.redis_publish_channel_key_name = redis_publish_channel_key_name
        self.logger.info("Attempting to connect to Redis pubsub")
        self.start_listener_and_flusher_thread()

    def start_listener_and_flusher_thread(self):
        if self.is_pubsub_listener_running or self.is_queue_flusher_running:
            self.logger.info("이미 thread 가 실행중")
            return
        
        self.is_stopped = False

        self.logger.info(f"Subscribing to channel: {self.redis_publish_channel_key_name}")
        self.pubsub.subscribe(self.redis_publish_channel_key_name)
        self.logger.info(f"Successfully subscribed to channel: {self.redis_publish_channel_key_name}")
                
        self.pubsub_listener_thread = threading.Thread(target=self.pubsub_listener)
        self.pubsub_listener_thread.daemon = True
        self.pubsub_listener_thread.start()
        self.logger.info("Started pubsub_listener_thread thread")

        self.queue_flusher_thread = threading.Thread(target=self.queue_flusher)
        self.queue_flusher_thread.daemon = True
        self.queue_flusher_thread.start()
        self.logger.info("Started queue_flusher_thread thread")

    def queue_flusher(self):
        self.is_queue_flusher_running = True
        while True:
            try:
                if self.is_stopped:
                    break
                self.flush_message_queue()
            except Exception as e:
                self.logger.error(f"Error in queue_flusher thread: {e}", exc_info=True)
                time.sleep(1)

    def flush_message_queue(self):
        message = self.message_queue.get(block=True, timeout=None)
        self.message_handler(message)

    def insert_test_message_into_message_queue(self, data):
        self.message_queue.put(
            {
                "type": "message",
                "pattern": None,
                "channel": 'UPBIT_NEW_NOTICES',
                "data": data
            }
        )

    def pubsub_listener(self):
        self.is_pubsub_listener_running = True
        pubsub = self.pubsub
        while True:
            try:
                if self.is_stopped:
                    break

                for message in pubsub.listen():
                    if message['type'] == 'message':
                        self.message_queue.put(message)
                        self.logger.info(f"Processed pubsub message: {message['data']}")
                    elif message['type'] == 'subscribe':
                        self.logger.info("Successfully subscribed to channel")
                    else:
                        self.logger.info(f"Received message of type: {message['type']}")
                        
            except Exception as e:
                self.logger.error(f"Error in pubsub thread: {e}", exc_info=True)
                time.sleep(2)


class TradingAgent:
    def __init__(self, bybit_api_key, bybit_secret_key, telegram_redis_client, INSTANCE_NAME):
        self.bybit_api_key = bybit_api_key
        self.bybit_secret_key = bybit_secret_key
        self.telegram_redis_client = telegram_redis_client
        self.INSTANCE_NAME = INSTANCE_NAME

        # Bybit 클라이언트 초기화 (테스트코드 기반)
        self.bybit_client = HTTP(
            api_key=self.bybit_api_key,
            api_secret=self.bybit_secret_key,
            testnet=False
        )

        # 초기 잔고 조회
        self.spot_balance_dict = self.get_amount_dict_in_bybit_spot()
        balance_dict = self.get_filtered_amount_dict_in_bybit_spot()
        self.send_messsage_to_telegram(f"TA 시작: {balance_dict}")

    def send_messsage_to_telegram(self, msg):
        now_dt = datetime.datetime.now(tz=pytz.timezone("Asia/Seoul"))
        now_dt_str = now_dt.isoformat()
        notice_data = {
                "level": "INFO",
                "time": now_dt_str,
                "message": f"{self.INSTANCE_NAME}\n{msg}\n"
        }
        self.telegram_redis_client._execute_xadd("NOTICE_STREAM:RUA_UB_BN_LISTING", value_dict=notice_data)

    def update_amount_dict_in_bybit_spot(self):
        self.spot_balance_dict = self.get_amount_dict_in_bybit_spot()

    def get_amount_dict_in_bybit_spot(self):
        """Bybit Unified 계정 잔고 조회 후 dict로 반환"""
        response = self.bybit_client.get_wallet_balance(accountType="UNIFIED")
        amount_dict = {}
        if response['retCode'] == 0:
            # 각 account에 대해 순회
            for account in response['result']['list']:
                for c in account['coin']:
                    wallet_balance = float(c.get('walletBalance', 0))
                    if wallet_balance > 0:
                        amount_dict[c['coin']] = str(wallet_balance)
        else:
            print(f"Bybit 잔고 조회 실패: {response['retMsg']}")
        return amount_dict

    def get_filtered_amount_dict_in_bybit_spot(self):
        filtered_dict = {}
        for k, v in self.spot_balance_dict.items():
            if k != 'USDT':
                # 0.0 형태의 자산 걸러내기
                if v in ('0.00000000', '0.00', '0.0', '0'):
                    continue
            filtered_dict[k] = v
        return filtered_dict

    def buy_market_order_in_bybit_spot(self, order_currency, payment_currency, value_in_payment_currency):
        import math

        usdt_balance = float(value_in_payment_currency)

        usdt_to_use = math.floor(usdt_balance * 100) / 100.0
        if usdt_to_use <= 0:
            # 잔고가 부족한 경우도 그냥 빈 문자열이나 response 없이 반환 (기존 로직대로 처리)
            return ""

        qty_str = str(usdt_to_use)

        order_resp = self.bybit_client.place_order(
            category="spot",
            symbol=f"{order_currency}{payment_currency}",
            side="Buy",
            orderType="MARKET",
            qty=qty_str,
            marketUnit="quoteCoin"
        )

        # 기존 바이낸스 로직과 동일하게 response를 그대로 문자열로 반환
        return str(order_resp)


    def message_handler(self, message: dict):
        try:
            print(f"message_handler is called with message: {message} type(message): {type(message)}")

            notice_data_str = message['data']

            if type(notice_data_str) != str:
                print(f"notice_data_str is not a str type. notice_data_str: {notice_data_str} / type(notice_data_str): {type(notice_data_str)}")
                return

            try:
                notice_data = json.loads(notice_data_str)
            except Exception as inner_e:
                print(f"json parsing 실패 notice_data_str: {notice_data_str}")
                return

            if notice_data.get('category') == 'test':
                return

            notice_title = notice_data.get('title', '')
            if '에어드랍' in notice_title or '이벤트' in notice_title:
                return

            self.send_messsage_to_telegram(f"message: {message}")

            if notice_data['action'] != "NEW":
                return

            notice_exchange = notice_data['exchange']
            usdt_amount_in_spot_wallet = self.spot_balance_dict.get('USDT', '0') 

            if notice_exchange == 'BITHUMB':
                from decimal import Decimal
                half_amount = Decimal(usdt_amount_in_spot_wallet) / Decimal('2')
                usdt_amount_in_spot_wallet = str(half_amount)

            order_currency_list = self.extract_order_currency_list_to_buy(notice_exchange, notice_title)
            print("order_currency_list", order_currency_list)
            
            result_list = []
            for this_oc in order_currency_list:
                try:
                    result = self.buy_market_order_in_bybit_spot(this_oc, 'USDT', usdt_amount_in_spot_wallet)
                except Exception as inner_e:
                    result = f"\n\n{this_oc} exception occurred. inner_e: {inner_e} skipped...\n\n"

                result_list.append(result)
            print("result_list", result_list)
            result_str = "\n".join(result_list)
            self.send_messsage_to_telegram(result_str)
            self.update_amount_dict_in_bybit_spot()

        except Exception as e:
            print("message_handler exception 발생!!!", e)

    def extract_order_currency_list_to_buy(self, notice_exchange, notice_title):
        if notice_exchange == 'UPBIT':
            if ("Market Support for" in notice_title or 
                "신규 거래지원 안내" in notice_title or 
                "디지털 자산 추가" in notice_title):
                pattern = r'(\w+)\(([^)]+)\)'
                matches = re.findall(pattern, notice_title)
                crypto_names = []
                for match in matches:
                    word1, word2 = match[0], match[1]
                    if word1.isupper():
                        crypto_names.append(word1)
                    elif word2.isupper():
                        crypto_names.append(word2)
                return crypto_names

        elif notice_exchange == 'BITHUMB' and "원화 마켓 추가" in notice_title:
            pattern = r'\((\w+)\)'
            matches = re.findall(pattern, notice_title)
            return matches
        
        return []


if __name__ == '__main__':
    from pub_sub_manager import PubSubManager
    from redis_client.settings import RedisSettingsManager

    BYBIT_API_KEY = os.environ["BYBIT_API_KEY"]
    BYBIT_API_SECRET = os.environ["BYBIT_API_SECRET"]
    INSTANCE_NAME = os.environ["INSTANCE_NAME"]


    ss = {
        "service_namespace": "zoo",
        "service_name": "kabigon",
        "service_instance_id": "001",
        "service_version": "0.0.1"
    }

    env_file_path = pathlib.Path(__file__).parent.parent / ".env"
    rsm = RedisSettingsManager(env_file=env_file_path)
    rs = rsm.redis_settings_map["BJS_NOTICE_PUBSUB"]
    psm = PubSubManager(ss, rs)
    rs = rsm.redis_settings_map["RUA_COMMON_LISTING"]

    ss = {
        "service_namespace": "zoo",
        "service_name": "telegram-reporter",
        "service_instance_id": "002",
        "service_version": "0.0.1"
    }

    telegram_redis_client = PubSubManager(ss, rs)

    ta = TradingAgent(BYBIT_API_KEY, BYBIT_API_SECRET, telegram_redis_client, INSTANCE_NAME)

    redis_publish_channel_key_name = f"CF_NEW_NOTICES"
    psm.prepare_pubsub(ta.message_handler)
    psm.subscribe(redis_publish_channel_key_name)

    i = 0
    while True:
        if i > 0:
            if i % 300 == 0:
                ta.update_amount_dict_in_bybit_spot()
                balance_dict = ta.get_filtered_amount_dict_in_bybit_spot()

            if i % 3600 == 0:            
                ta.send_messsage_to_telegram(f"현재 SPOT balance: {balance_dict}")

        time.sleep(1)
        i += 1
