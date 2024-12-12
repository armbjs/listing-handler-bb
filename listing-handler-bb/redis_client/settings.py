

import pathlib
import os
import typing
import json
import dotenv
import pathlib
import typing
import json
import casestyle
import os
from pydantic_settings import BaseSettings, SettingsConfigDict # pip install pydantic-settings
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator  # pip install pydantic




# current_dir_path = pathlib.Path(__file__).resolve().parent
# print(current_dir_path / '.env')

def case_insensitive_validator(field: str):
    return field.lower()

class RedisSettings(BaseModel):
    model_config = ConfigDict(
        extra='allow',
        alias_generator=case_insensitive_validator,
    )
        
    host: str
    port: int = 16752
    db_index: int = 0
    username: str
    password: str
    timeout: float = 1.0
    is_ssl: bool = False
    ssl_ca_data: typing.Optional[str] = None
    

    @model_validator(mode='before')
    def lowercase_keys(cls, values):
        return {k.lower(): v for k, v in values.items()}

    @field_validator('is_ssl', mode='before')
    def parse_is_ssl(cls, v):
        if isinstance(v, str):
            return v.lower() in ('true', '1', 'yes')
        return bool(v)

    @field_validator('port', 'db_index', mode='before')
    def parse_int(cls, v):
        if isinstance(v, str):
            return int(v)
        return v

    @field_validator('ssl_ca_data')
    def check_ssl_ca_data(cls, v, info):
        is_ssl = info.data.get('is_ssl')
        if is_ssl:
            if not v:
                raise ValueError('ssl_ca_data is required when is_ssl is True')
            v = v.strip()
            if not v.startswith('-----BEGIN CERTIFICATE-----') or not v.endswith('-----END CERTIFICATE-----'):
                raise ValueError("Invalid SSL certificate data")
        return v
    

    def get_redis_url(self):
        scheme = 'rediss' if self.is_ssl else 'redis'
        return f"{scheme}://{self.username}:{self.password}@{self.host}:{self.port}/{self.db_index}"

    def get_redis_client_format_params(self):
        return {
            'host': self.host,
            'port': self.port,
            'db': self.db_index,
            'username': self.username,
            'password': self.password,
            'socket_timeout': self.timeout,
            'ssl': self.is_ssl,
            "ssl_ca_data": self.ssl_ca_data# .replace("\\n", "\n")


        }
    

    @classmethod
    def generate_empty_settings_params(cls) -> dict:
        return {
            "REDIS_KEY_NAMESPACE": "DEBUGGING",
            "HOST": 'aaa.com',
            "PORT": '16752',
            "DB_INDEX": '0',
            "USERNAME": 'aaa',
            "PASSWORD": 'bbb',
            "IS_SSL": 'True',
            "SSL_CA_DATA": """-----BEGIN CERTIFICATE-----aaa-----END CERTIFICATE-----"""
        }
    
class RedisSettingsManager(BaseSettings):

    model_config = SettingsConfigDict(
        # env_file=current_dir_path / ".env", # 여기서 env_file 을 지정하면, 생성자에서 _env_file 로 읽을 _env_file 을 지정할 수 없어진다.
        env_file_encoding='utf-8',
        env_prefix='',
        extra='allow',
    )

    redis_settings_map: typing.Dict[str, RedisSettings] = Field(default_factory=dict)

    def parse_redis_configs(self):
        configs = {}
        
        model_extra = self.model_extra
        # os.environ
        for key, value in os.environ.items():
            key_in_uppercase = key.upper()
            if key_in_uppercase.startswith('REDIS_CONFIG_'):
                suffix = key_in_uppercase.split('REDIS_CONFIG_', 1)[1]
                try:
                    # https://stackoverflow.com/questions/31203259/python-write-valid-json-with-newlines-to-file
                    # If strict is false (True is the default),
                    # then control characters will be allowed inside strings.
                    # 
                    parsed_value = json.loads(value, strict=False)
                    configs[suffix] = RedisSettings(**parsed_value)
                    print(f"Created RedisSettings for {suffix}")
                except json.JSONDecodeError:
                    print(f"Failed to parse JSON for {key_in_uppercase}")
                    raise ValueError(f"Invalid JSON in environment variable {key_in_uppercase}")
                except Exception as e:
                    print(f"Error creating RedisSettings for {suffix}: {str(e)}")
                    raise
                
        for key, value in model_extra.items():
            key_in_uppercase = key.upper()
            if key_in_uppercase.startswith('REDIS_CONFIG_'):
                suffix = key_in_uppercase.split('REDIS_CONFIG_', 1)[1]
                try:
                    # https://stackoverflow.com/questions/31203259/python-write-valid-json-with-newlines-to-file
                    # If strict is false (True is the default),
                    # then control characters will be allowed inside strings.
                    # 
                    parsed_value = json.loads(value, strict=False)
                    configs[suffix] = RedisSettings(**parsed_value)
                    print(f"Created RedisSettings for {suffix}")
                except json.JSONDecodeError:
                    print(f"Failed to parse JSON for {key_in_uppercase}")
                    raise ValueError(f"Invalid JSON in environment variable {key_in_uppercase}")
                except Exception as e:
                    print(f"Error creating RedisSettings for {suffix}: {str(e)}")
                    raise
        self.redis_settings_map = configs

    def __init__(self, *args, **kwargs):
        # SettingsConfigDict 에 env_file 을 지정하면, 생성자에 _env_file 을 넘겨도 무시된다.

        # 사용자가 _env_file 값을 지정했다면 그걸 쓰고, 아니면 cwd 를 사용한다.
        # load_dotenv 의 기본 .env 검색 동작이, 현재 directory 부터 상위로 계속 올라가면서 .env 파일을 찾는 것
        # env_file = kwargs.get('_env_file', pathlib.Path(os.getcwd()) / '.env')
         
        # .env 파일을 읽어서 os.environ 에 추가하진 않는다.
        # os.environ 에 .env 파일의 내용을 추가한다.
        # 이러면 내부적으로 os.environ 값을 사용하게 된다.
        #dotenv.load_dotenv(env_file, override=True)

        super().__init__(*args, **kwargs)

        self.parse_redis_configs()



def main():
    os.environ.clear()

    env_file_path = pathlib.Path(__file__).parent.parent / ".env"
    redis_settings_manager = RedisSettingsManager(_env_file=env_file_path)
    
    for name, config in redis_settings_manager.redis_settings_map.items():
        print(f"{name}: {config.get_redis_url()}")
    print("done")
    
if __name__ == '__main__':
    main()