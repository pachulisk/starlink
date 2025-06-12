from upstash_redis import Redis
from .config import Config
import os 

env = os.environ.get('env')
# if env is empty, set env to "test"
if env is None:
    env = "test"

cfg = Config(env)

url: str = cfg.UPSTASH_URL
key: str = cfg.UPSTASH_KEY

print("upstash url = ", url)
print("upstash key = ", key)

redis = Redis(url="url", token=key)
