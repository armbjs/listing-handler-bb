"""

2024-04-02 16:02
* 아래 파일 추가

2024-07-09 17:28
* get_package_name() 추가

"""
import json
import pathlib
from typing import Dict, Any


def get_version() -> str:
    current_file_dir_path: pathlib.Path = pathlib.Path(__file__).parent.resolve()
    # Open is relative to the current directory and does not use PYTHONPATH
    with open(current_file_dir_path / "version.json", mode="rt", encoding="utf-8") as f:
        version_dict: Dict[str, Any] = json.loads(f.read())
    return version_dict["version"]


def get_package_name() -> str:
    current_file_dir_path: pathlib.Path = pathlib.Path(__file__).parent.resolve()
    # Open is relative to the current directory and does not use PYTHONPATH
    with open(current_file_dir_path / "version.json", mode="rt", encoding="utf-8") as f:
        version_dict: Dict[str, Any] = json.loads(f.read())
    return version_dict["package-name"]
