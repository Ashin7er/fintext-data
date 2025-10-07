# -*- coding: utf-8 -*-
"""
从新浪接口获取沪深300成分（node=hs300），仅用标准库。
输出: data/universe.csv（列: ticker,name）
ticker 格式：上证 .SS，深证 .SZ
用法：
  python tools/fetch_universe_sina_hs300.py
"""

import csv, json, time
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DATA.mkdir(parents=True, exist_ok=True)

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"

def get_page(node: str, page: int, num: int = 80, sort: str = "symbol", asc: int = 1):
    """
    新浪市场中心接口（JSON）：节点=hs300
    例如：
    https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?node=hs300&num=80&page=1
    常见字段：symbol(sh600000), name(浦发银行), code(600000) 等
    """
    base = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData"
    url = f"{base}?{urlencode({'page': page, 'num': num, 'node': node, 'sort': sort, 'asc': asc})}"
    req = Request(url, headers={"User-Agent": UA, "Connection": "close"})
    with urlopen(req, timeout=20) as resp:
        txt = resp.read().decode("utf-8", errors="ignore").strip()
        try:
            data = json.loads(txt)
        except Exception:
            return []
        return data if isinstance(data, list) else []

def normalize_symbol(symbol: str):
    # symbol 形如 sh600000 / sz000001
    if not symbol or len(symbol) < 3:
        return None
    ex = symbol[:2].lower()
    code = symbol[2:]
    if ex == "sh":
        return f"{code}.SS"
    elif ex == "sz":
        return f"{code}.SZ"
    else:
        return None

def main():
    node = "hs300"
    page = 1
    out = []
    seen = set()
    while True:
        try:
            arr = get_page(node=node, page=page, num=80)
        except (URLError, HTTPError) as e:
            print(f"[WARN] page {page} 请求失败：{e}，暂停重试")
            time.sleep(1.0)
            try:
                arr = get_page(node=node, page=page, num=80)
            except Exception as e2:
                print(f"[WARN] page {page} 重试仍失败：{e2}，结束抓取")
                break
        if not arr:
            break
        for it in arr:
            symbol = (it.get("symbol") or "").lower()
            name = (it.get("name") or "").strip()
            ticker = normalize_symbol(symbol)
            if ticker and ticker not in seen:
                out.append((ticker, name if name else ticker))
                seen.add(ticker)
        print(f"[INFO] 抓到 {len(arr)} 条（累计 {len(out)}）")
        page += 1
        time.sleep(0.2)

    if not out:
        demo = [("000001.SZ","平安银行"), ("600036.SS","招商银行"), ("600519.SS","贵州茅台")]
        out = demo
        print("[WARN] 未从新浪取到列表，写入示例 3 条；建议稍后重试。")

    path = DATA/"universe.csv"
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["ticker","name"])
        for t, n in out:
            w.writerow([t, n])

    print(f"[OK] 写出 {path} ，共 {len(out)} 只（正常应接近 300）")

if __name__ == "__main__":
    main()
