from supabase import create_client, Client
from .config import Config
import os 

env = os.environ.get('env')
# if env is empty, set env to "test"
if env is None:
    env = "test"

cfg = Config(env)

url: str = cfg.SUPABASE_URL
key: str = cfg.SUPABASE_KEY

print("url = ", url)
print("key = ", key)


supabase: Client = create_client(url, key)