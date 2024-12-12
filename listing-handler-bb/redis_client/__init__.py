"""
__version__ variable is set automatically according to the version in pyproject.toml

16:02 2024-04-02
* version.py 파일의 get_version 함수를 이용해서, 현재 version 을 파악하도록 수정
* __package__ 값과 __version__ 값을 처음 실행시 print 하도록 수정

"""
# https://github.com/python-poetry/poetry/issues/1652
# https://github.com/python-poetry/poetry/issues/144

from .version import get_version

__version__ = get_version()
print("__package__ and __version__", __package__, __version__)
del get_version

from .real_redis_client_interface import RealRedisClientInterface
from .file_redis_client_interface import FileRedisClientInterface
from .settings import RedisSettingsManager, RedisSettings


__all__ = ["__version__"]
