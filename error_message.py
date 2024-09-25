import base64
import hashlib
import hmac
from datetime import datetime

import pytz
import requests

# 正式环境
WEBHOOK_URL = "https://open.larksuite.com/open-apis/bot/v2/hook/b4164b01-2d4a-461d-90dc-d54a2f843c24"
WEBHOOK_SECRET = "pbEQbitncvDQ7npSOpwtDc"
# 测试环境
# WEBHOOK_URL = "https://open.larksuite.com/open-apis/bot/v2/hook/4369cd93-e94b-4d00-9c20-bfb0924d7b0e"
# WEBHOOK_SECRET = "C6Iq5S66oiXh5Iuo6rUCzd"


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


def send_error_a_message(content):
    sign = gen_sign(WEBHOOK_SECRET)
    shanghai_timezone = pytz.timezone('Asia/Shanghai')
    timestamp = int(datetime.now(shanghai_timezone).timestamp())

    time_str = datetime.now(shanghai_timezone).strftime("%Y-%m-%d %H:%M:%S")  # 格式化时间字符串

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
