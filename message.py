import base64
import hashlib
import hmac
from datetime import datetime

import requests

WEBHOOK_URL = "https://open.larksuite.com/open-apis/bot/v2/hook/ad934e25-2ee5-4cfc-8ef4-2482361c3b95"
WEBHOOK_SECRET = "L69pb5WOpFWFbyhD63CI0c"


def gen_sign(secret):  # 拼接时间戳以及签名校验
    timestamp = int(datetime.now().timestamp())
    string_to_sign = '{}\n{}'.format(timestamp, secret)

    # 使用 HMAC-SHA256 进行加密
    hmac_code = hmac.new(
        string_to_sign.encode("utf-8"), digestmod=hashlib.sha256
    ).digest()

    # 对结果进行 base64 编码
    sign = base64.b64encode(hmac_code).decode('utf-8')

    return sign


def send_a_message(content):
    sign = gen_sign(WEBHOOK_SECRET)
    timestamp = int(datetime.now().timestamp())
    time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # 格式化时间字符串
    params = {
        "timestamp": timestamp,
        "sign": sign,
        "msg_type": "text",
        "content": {"text": f"{time_str}\n{content}"},
    }

    resp = requests.post(WEBHOOK_URL, json=params)
    resp.raise_for_status()
    result = resp.json()
    if result.get("code") and result.get("code") != 0:
        print(f"发送失败：{result['msg']}")
        return
    print("消息发送成功")
