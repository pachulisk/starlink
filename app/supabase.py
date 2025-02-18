from supabase import create_client, Client
from .config import Config
from datetime import datetime
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

async def get_supabase_table_latest_row(table_name, gwid, column="happendate"):
    response = supabase.table(table_name).select(column).eq("gwid", gwid).order(column, desc=True).limit(1).execute()
    data = response.data
    if len(data) <= 0:
        return None
    else:
        return data[0].get(column)
    
def build_sql_for_latest_row(table_name, column="happendate"):
    return f"SELECT {column} FROM {table_name} ORDER BY {column} DESC LIMIT 1"

def get_db_for_table(table_name):
    if  table_name in ["ipreport", "acctreport"]:
        return "ISP.db"
    elif table_name in ["hourreport", "webreport", "protocolreport", "protocolreport_today"]:
        return "report.db"
    else: 
        return "wfilter.db"
    
def meta_for_table_name(table_name):
    meta_mapping = {
        "hourreport": "happendate|hour|uptraffic|downtraffic|",
        "ipreport": "happendate|ip|uptraffic|downtraffic|",
        "acctreport": "happendate|acct|uptraffic|downtraffic|",
        "webreport": "ip|group1|acct|happendate|host|category1|category2|visitcnt|uptraffic|downtraffic|during|",
        "webreport_today": "ip|group1|acct|happendate|host|category1|category2|visitcnt|uptraffic|downtraffic|during|",
        "protocolreport": "ip|group1|acct|happendate|category|protocol|uptraffic|downtrafficduring|",
        "protocolreport_today": "ip|group1|acct|happendate|category|protocol|uptraffic|downtrafficduring|",
        "sessionslog": "ip|group1|acct|mac|happentime|direction|proto|target|cmd|remark|",
        "ftplog": "ip|group1|acct|mac|happentime|direction|type|target|filesize|refer|filename|title|useragent|fileid|remark|targetip|",
        "ipmaclog": "ip|group1|acct|mac|happentime|hostname|",
        "maillog": "ip|group1|acct|mac|happentime|direction|fromid|toid|subject|messageid|fileid|proto|remark|targetip|",
        "webpostlog": "ip|group1|acct|mac|happentime|host|webtitle|postsize|posturl|fileid|refer|useragent|tls|remark|targetip|",
        "websurflog": "ip|group1|acct|mac|happentime|host|url|webtitle|tls|useragent|remark|targetip|"
    }
    if table_name in meta_mapping:
        return meta_mapping[table_name]
    else:
        return None
    
def formalize_supabase_datetime(dt):
    d = dt
    # regex = re.compile(r'T')
    # if regex.match(d):
    index = d.find('T')
    if index != -1:
        d = dt[:index]
    return d

def make_list(row):
    my_lst = row.split("|")
    return list(filter(lambda x: len(x)>0, my_lst))


def build_dict_from_line(meta, line):
    """
    meta: happendate|hour|uptraffic|downtraffic|
    line: 2024-11-07|14|5772895|21432009|
    """
    dict = {}
    meta_list = make_list(meta)
    meta_len = len(meta_list)
    data_list = make_list(line)
    data_len = len(data_list)
    length = min(meta_len, data_len)
    for i in range(length):
        dict[meta_list[i]] = data_list[i]
    return dict

def to_date(data):
    print(f"to_date: {data}")
    d = data
    # regex = re.compile(r'T')
    # if regex.match(d):
    index = d.find('T')
    if index != -1:
        d = data[:index]
    print(f"to_date, after process d = {d}")
    processed = datetime.strptime(d, "%Y-%m-%d")
    print(f"processed = {processed}")
    return processed

def str_strip(data):
    if data is None:
        return None
    else:
        ret = data.replace("\n", "")
        ret = ret.replace("|", "")
        return ret