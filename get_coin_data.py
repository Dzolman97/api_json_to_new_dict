from pprint import pprint
from requests import Request, Session
from requests. exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import psycopg2
from psycopg2.extras import execute_batch


url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
parameters = {
  'start':'1',
  'limit':'100',
  'convert':'USD'
}
headers = {
  'Accepts': 'application/json',
  'X-CMC_PRO_API_KEY': '',
}

session = Session()
session.headers.update(headers)

res_data = []

try:
  response = session.get(url, params=parameters)
  data = json.loads(response.text)
  res_data.append(data)
except (ConnectionError, Timeout, TooManyRedirects) as e:
  print(e)


def get_coins():
   return res_data


def latest_data():
    listed_coins = get_coins()

    out_of_list = listed_coins[0]

    new_list = []

    for data in out_of_list['data']:
       my_dict = {}

       my_dict['coin_name'] = data['name']
       my_dict['coin_symbol'] = data['symbol']
       my_dict['coin_price'] = data['quote']['USD']['price']
       my_dict['volume_24h'] = data['quote']['USD']['volume_24h']
       my_dict['volume_change_24h'] = data['quote']['USD']['volume_change_24h']
       my_dict['percent_change_1h'] = data['quote']['USD']['percent_change_1h']
       my_dict['percent_change_24h'] = data['quote']['USD']['percent_change_24h']
       my_dict['percent_change_7d'] = data['quote']['USD']['percent_change_7d']
       my_dict['percent_change_30d'] = data['quote']['USD']['percent_change_30d']
       my_dict['percent_change_60d'] = data['quote']['USD']['percent_change_60d']
       my_dict['percent_change_90d'] = data['quote']['USD']['percent_change_90d']
       
       new_list.append(my_dict)

    return new_list



hostname = ''
database = ''
username = ''
pwd = ''
port_id = 5432
conn = None
cur = None

try:
    conn = psycopg2.connect(
                host = hostname,
                dbname = database,
                user = username,
                password = pwd,
                port = port_id)
    
    cur = conn.cursor()

    values = latest_data()
    query = "INSERT INTO latest_coin_data VALUES (%(coin_name)s, %(coin_symbol)s, %(coin_price)s, %(volume_24h)s, %(volume_change_24h)s, %(percent_change_1h)s, %(percent_change_24h)s, %(percent_change_7d)s, %(percent_change_30d)s, %(percent_change_60d)s, %(percent_change_90d)s)"
    
    execute_batch(cur, query, values)

    conn.commit()
except Exception as error:
    print(error)
finally:
    if cur is not None:
       cur.close()
    if conn is not None:
       conn.close()
