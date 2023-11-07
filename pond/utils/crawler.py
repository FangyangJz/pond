from random import choice

import requests


def request_session():
    ###########################
    # fangyang add in case ban req
    user_agent_list = [
        "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; WOW64) Gecko/20100101 Firefox/61.0",
        "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.62 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0)",
        "Mozilla/5.0 (Macintosh; U; PPC Mac OS X 10.5; en-US; rv:1.9.2.15) Gecko/20110303 Firefox/3.6.15",
    ]

    # 频繁请求某个网址偶尔会报错请求超时，可采用下面方式降低失败率，设置重试次数为5，会话设置为不维持连接.
    requests.adapters.DEFAULT_RETRIES = 5
    ses = requests.session()
    ses.keep_alive = False

    headers = {
        'User-Agent': choice(user_agent_list),
        'Connection': 'close'
    }

    # proxies = {
    #     "http": "http://127.0.0.1:1080",
    #     "https": "https://127.0.0.1:1080"
    # }
    ses.headers.update(headers)
    ###################

    return ses