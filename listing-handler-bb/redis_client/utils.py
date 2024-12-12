import re
import pathlib
import dotenv
import json
import os

class dotdict(dict):

    def __getattr__(self, attr):
        return self[attr]

    # def __missing__(self, key):
    #     self[key] = dotdict()
    #     return self[key]

    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


def is_valid_redis_key(s):
    # 시작과 끝이 : 이면 안된다.
    if s[0] == ':' or s[-1] == ':':
        return False
    # 시작과 끝이 _ 이면 안된다.
    if s[0] == '_' or s[-1] == '_':
        return False
    # __ 혹은 :: 혹은 _: 혹은 :_ 가 있으면 안된다.
    if '__' in s or '::' in s or '_:' in s or ':_' in s:
        return False
    
    return bool(re.match(r'^[A-Z_0-9:]+$', s))



def env_to_json_str(env_file_path='.env'):
    dotenv_values = dotenv.dotenv_values(env_file_path)
    json_str = json.dumps(dotenv_values)
    return json_str

# Example usage
if __name__ == "__main__":
    json_str = env_to_json_str(
        pathlib.Path(__file__).parent.parent / '.env'
    )

    print("json_str", json_str)