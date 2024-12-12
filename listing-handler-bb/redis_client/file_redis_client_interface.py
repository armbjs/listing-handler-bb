import typing
import types
import json
import pathlib

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

class FileRedisClientInterface(RedisClientInterface):

    def __init__(self, service_settings, _connection_info: settings.RedisSettings, logger_name='root', verbose=False):
        self.is_multiprocess_client_initialized = False
        super().__init__(service_settings, _connection_info, logger_name, verbose)

        
        self.internal_redis_data_file_path = pathlib.Path(__file__).parent.resolve() / "redis_store.json"
        with open(self.internal_redis_data_file_path, 'wt', encoding='utf-8') as f:
            f.write("{}")

        if self.verbose:
            if self.redis_client_info:
                print("\nredis_client_info")
                for k, v in self.redis_client_info.items():
                    if k in ['password']:
                        continue
                    print(k, v)

            print("\n")


    def close(self):
        pass


    def _execute_get(self, sub_key):
        full_key = self._generate_namespaced_key(sub_key)
        with open(self.internal_redis_data_file_path, 'rt', encoding='utf-8') as f:
            full_data_str = f.read()
        full_data = json.loads(full_data_str)
        result = full_data.get(full_key, None)

        return {
            "result": result,
            "full_key": full_key,
        }

    def _execute_set(self, sub_key, value):
        full_key = self._generate_namespaced_key(sub_key)
        
        with open(self.internal_redis_data_file_path, 'rt', encoding='utf-8') as f:
            
            full_data_str = f.read()
     
        full_data = json.loads(full_data_str)
        full_data[full_key] = value
        new_full_data = json.dumps(full_data, ensure_ascii=False, indent=4)
        with open(self.internal_redis_data_file_path, 'wt', encoding='utf-8') as f:
            f.write(new_full_data)
     
        return {
            "result": True,
            "full_key": full_key,
        }

    def _execute_xrange(self, sub_key, start_stream_id=None, end_stream_id=None, count=None):
        raise NotImplementedError("_execute_xrange is not implemented for FileRedisClientInterface")
    
    def _execute_xrevrange_to_get_latest_n_entries(self, sub_key, count):
        raise NotImplementedError("_execute_xrevrange_to_get_latest_n_entries is not implemented for FileRedisClientInterface")