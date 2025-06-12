import os

SUPABASE_KEY = os.getenv('SUPABASE_KEY')
SUPABASE_URL = os.getenv('SUPABASE_URL')
traffic_ratio = os.getenv('TRAFFIC_RATIO')
UPSTASH_URL = os.getenv('UPSTASH_URL')
UPSTASH_KEY = os.getenv('UPSTASH_KEY')

class Config:
  def __init__(self, env):
    if env == "test":
        self.SUPABASE_URL = SUPABASE_URL
        self.SUPABASE_KEY = SUPABASE_KEY
        self.TRAFFIC_RATIO = traffic_ratio
        self.UPSTASH_URL = UPSTASH_URL
        self.UPSTASH_KEY = UPSTASH_KEY
        # self.user = "test_user"
        # self.password = "test_password"
    elif env == "prod" or env == "production":
        self.SUPABASE_URL = SUPABASE_URL
        self.SUPABASE_KEY = SUPABASE_KEY
        self.TRAFFIC_RATIO = traffic_ratio
        self.UPSTASH_URL = UPSTASH_URL
        self.UPSTASH_KEY = UPSTASH_KEY