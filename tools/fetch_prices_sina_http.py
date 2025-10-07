# -*- coding: utf-8 -*-
"""
新浪历史日K抓取，标准库实现。
输入：data/universe.csv (列: ticker)
输出：data/prices_multi/{TICKER}_prices.csv (dt,open,high,low,close,volume,ticker)
以及 data/prices_all.csv
"""
import csv, json, time
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DATA.mkdir(parents=True, exist_ok=True)

def load_universe():
    tickers = []
    with open(DATA/"universe.csv", newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            tickers.append(row["ticker"])
    return tickers

def sina_kline(symbol: str, scale: int = 240, datalen: int = 3000):
    """
    新浪K线JSON接口：scale=240 表示日K。
    symbol: sz000001 / sh600000
    """
    base = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketDataService.getKLineData"
    params = {"symbol": symbol, "scale": scale, "ma": "no", "datalen": datalen}
    url = f"{base}?{urlencode(params)}"
    req = Request(url, headers={"User-Agent":"Mozilla/5.0"})
    with urlopen(req, timeout=20) as resp:
        txt = resp.read().decode("utf-8").strip()
        data = json.loads(txt) if txt.startswith("[") else []
        out = []
        for it in data:
            day = it.get("day")
            if not day: 
                continue
            out.append([
                day+" 15:00:00",
                it.get("open"), it.get("high"), it.get("low"), it.get("close"),
                it.get("volume")
            ])
        return out

def save_price_csv(rows, path, ticker):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["dt","ticker","open","high","low","close","volume"])
        for r in rows:
            w.writerow([r[0], ticker, r[1], r[2], r[3], r[4], r[5]])

def main():
    tickers = load_universe()
    all_rows = []
    for tic in tickers:
        raw = tic.split(".")[0]
        ex = "sh" if tic.endswith(".SS") else "sz"
        symbol = f"{ex}{raw}"
        try:
            rows = sina_kline(symbol, scale=240, datalen=3000)
        except (URLError, HTTPError) as e:
            print("[WARN]", tic, e); rows=[]
        if rows:
            save_price_csv(rows, DATA/"prices_multi"/f"{tic}_prices.csv", tic)
            for r in rows:
                all_rows.append([r[0], r[1], r[2], r[3], r[4], r[5], tic])
            print(f"[OK] {tic} prices: {len(rows)}")
        else:
            print(f"[WARN] {tic} 无价格")
        time.sleep(0.25)
    if all_rows:
        with open(DATA/"prices_all.csv", "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            w.writerow(["dt","open","high","low","close","volume","ticker"])
            for r in all_rows:
                w.writerow(r)
        print(f"[OK] prices_all.csv -> {len(all_rows)} 行")

if __name__ == "__main__":
    main()
