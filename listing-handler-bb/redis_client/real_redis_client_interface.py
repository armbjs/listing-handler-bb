import typing
import types
import redis

if __package__ is None or __package__ == '':
    from redis_client_interface import RedisClientInterface
else:

    from .redis_client_interface import RedisClientInterface


if __package__ is None or __package__ == '':
    import utils
    import settings
else:
    from . import utils
    from . import settings

class RealRedisClientInterface(RedisClientInterface):

    def __init__(self, service_settings, _connection_info: settings.RedisSettings, logger_name='root', verbose=False):
        self.is_multiprocess_client_initialized = False
        super().__init__(service_settings, _connection_info, logger_name, verbose)

        if self.is_multiprocess_client_initialized:
            raise Exception("Redis Client cannot be used in different processes.")

        self.redis_client_info = _connection_info.get_redis_client_format_params()

        if self.verbose:
            if self.redis_client_info:
                print("\nredis_client_info")
                for k, v in self.redis_client_info.items():
                    if k in ['password']:
                        continue
                    print(k, v)

            print("\n")

        try:
            # 생성자 인자 리스트
            # https://redis.readthedocs.io/en/stable/connections.html#redis.Redis
            self.raw_redis_client = redis.Redis(
                **self.redis_client_info,
                retry_on_timeout=True,
                # socket_timeout=2,  # socket.settimeout(value) 으로 연결 된다. Set a timeout on blocking socket operations
                # retry_on_timeout 이 True 면 retry_on_error 에 해당 Exception 을 추가한다.
                # retry_on_error 가 빈 리스트가 아니면,
                # retry_on_error 리스트에 무조건 아래의 supported_errors 가 추가된다.
                # supported_errors=(ConnectionError, TimeoutError, socket.timeout),
                # 그리고, retry_on_error list 로 retry object 를 만들게 된다.
                # retry_on_timeout 이 True 기만 하면, retry_on_error == [] 여도,
                # retry_on_error 에 TimeoutError 가 추가되므로, 빈 list 가 아니게 되어서,
                # supported_errors 를 추가한 retry_on_error 로 retry object 를 만들게 된다.
                retry_on_error=[redis.exceptions.ConnectionError],
                decode_responses=True,
            )

            # 정상일 때는 True 를 리턴한다.

            self.raw_redis_client.ping()
            # 포트 잘 못 연결시
            # redis.exceptions.ConnectionError: Error 10060 connecting to test4.ruacm.net:63791. 연결된 구성원으로부터 응답이 없어 연결하지 못했거나, 호스트로부터 응답이 없어 연결이 끊어졌습니다.
            # 포트 잘 못 연결 해놓고, socket_timeout 값을 작게 잡았을 경우
            # redis.exceptions.TimeoutError: Timeout connecting to server
        except Exception as e:
            raise




    def close(self):
        self.raw_redis_client.close()


    def _execute_get(self, sub_key):
        full_key = self._generate_namespaced_key(sub_key)
        result = self.raw_redis_client.get(full_key)
        if result is None:
            raise Exception(f"Key not found: {full_key}")
        
        return {
            "result": result,
            "full_key": full_key,
        }

    def _execute_set(self, sub_key, value):
        full_key = self._generate_namespaced_key(sub_key)
        result = self.raw_redis_client.set(full_key, value)
        return {
            "result": result,
            "full_key": full_key,
        }

    

    def _execute_xrange(self, sub_key, start_stream_id=None, end_stream_id=None, count=None):
        full_key = self._generate_namespaced_key(sub_key)

        if start_stream_id is None and end_stream_id is None and count is None:
            raise Exception("Either start_stream_id, end_stream_id or count should be provided.")
        # 둘이 min, max 순서 반대
        # redis_client.xrange(stream_name, min='-', max='+', count=None)
        # redis_client.xrevrange(stream_name, max='+', min='-', count=None)
        result = self.raw_redis_client.xrange(
            full_key, start_stream_id, end_stream_id, count=count
        )
        return {
            "result": result,
            "full_key": full_key,
        }
    

    def _execute_xrevrange(self, sub_key, start_stream_id=None, end_stream_id=None, count=None):
        full_key = self._generate_namespaced_key(sub_key)

        if start_stream_id is None and end_stream_id is None and count is None:
            raise Exception("Either start_stream_id, end_stream_id or count should be provided.")
        # 둘이 min, max 순서 반대
        # redis_client.xrange(stream_name, min='-', max='+', count=None)
        # redis_client.xrevrange(stream_name, max='+', min='-', count=None)
        result = self.raw_redis_client.xrevrange(
            full_key, end_stream_id, start_stream_id, count=count
        )
        return {
            "result": result,
            "full_key": full_key,
        }
    
    def _execute_xadd(self, sub_key, value_dict={}):
        full_key = self._generate_namespaced_key(sub_key)
        print("full_key", full_key)
        result = self.raw_redis_client.xadd(full_key, value_dict)

        return {
            "result": result,
            "full_key": full_key,
        }
    


    def _execute_xrevrange_to_get_latest_n_entries(self, sub_key, count):
        full_key = self._generate_namespaced_key(sub_key)
        result = self.raw_redis_client.xrevrange(full_key, count=count)

        return {
            "result": result,
            "full_key": full_key,
        }