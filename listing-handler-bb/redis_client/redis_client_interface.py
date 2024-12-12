import typing
import types
import abc
import logging

if __package__ is None or __package__ == '':
    import utils
    import settings
else:
    from . import utils
    from . import settings


class RedisClientInterface(abc.ABC):

    @typing.final
    def __init__(self, service_settings: dict, _connection_info: settings.RedisSettings, logger_name='root', verbose=False):

        self.logger = logging.getLogger(logger_name)
        self.is_initialized = False
        self.connection_info = self._parse_connection_info(_connection_info)

        self.service_settings = service_settings

        self._set_redis_key_namespace(self.service_settings)

        self.verbose = verbose
        self._raw_redis_client = None
    
    @typing.final
    def _set_redis_key_namespace(self, service_settings):

        _redis_key_namespace = f"{service_settings['service_namespace']}:{service_settings['service_name']}:{service_settings['service_instance_id']}"
        _redis_key_namespace = _redis_key_namespace.upper()
        _redis_key_namespace = _redis_key_namespace.replace('-', '_')
                                                                                                     
        # : 으로 끝나면 exception 발생시킨다.
        if _redis_key_namespace[-1] == ':':
            raise Exception("redis_key_namespace should not end with ':'")
        
        # . 으로 끝나면 exception 발생시킨다.
        if _redis_key_namespace[-1] == '.':
            raise Exception("redis_key_namespace should not end with '.'")
        
        # [0-9A-Z_:] 이외의 문자가 있다면 exception 발생시킨다.
        if not utils.is_valid_redis_key(_redis_key_namespace):
            raise Exception("redis_key_namespace should only contain [0-9A-Z_:]")

        self.redis_key_namespace = _redis_key_namespace

    @typing.final
    def _parse_connection_info(self, _connection_info):

        return _connection_info

    @property
    def raw_redis_client(self):
        if not self.is_initialized:
            raise Exception("redis_client has yet to be initialized.")

        return self._raw_redis_client

    @raw_redis_client.setter
    def raw_redis_client(self, _raw_redis_client):
        self._raw_redis_client = _raw_redis_client
        self.is_initialized = True

    @typing.final   
    def _generate_namespaced_key(self, key):
        # key 가 : 로 시작하면 exception 발생시킨다.

        if not utils.is_valid_redis_key(key):
            raise Exception(f"Invalid redis key: {key}")
        
        return f"{self.redis_key_namespace}:{key}"
    
    @abc.abstractmethod
    def close(self):
        raise NotImplementedError
    
    # @abc.abstractmethod # 반드시 구현될 필욘 없다
    def _execute_get(self, key):
        raise NotImplementedError
    
    # @abc.abstractmethod # 반드시 구현될 필욘 없다
    def _execute_set(self, key, value):
        raise NotImplementedError
    
    # @abc.abstractmethod # 반드시 구현될 필욘 없다
    def _execute_xadd(self, key, data={}):
        raise NotImplementedError
    
    # @abc.abstractmethod # 반드시 구현될 필욘 없다
    def _execute_xrange(self, key):
        raise NotImplementedError
    
    # @abc.abstractmethod # 반드시 구현될 필욘 없다
    def _execute_xrevrange(self, key):
        raise NotImplementedError
    
    # @abc.abstractmethod # 반드시 구현될 필욘 없다
    def _execute_xrevrange_to_get_latest_n_entries(self, key, count):
        raise NotImplementedError