import os


def env_int(name, default=None):
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        return default
    try:
        return int(value)
    except ValueError:
        print(f"⚠️ 忽略无效整数环境变量 {name}={value!r}")
        return default


def env_float(name, default):
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        return default
    try:
        return float(value)
    except ValueError:
        print(f"⚠️ 忽略无效数字环境变量 {name}={value!r}")
        return default
