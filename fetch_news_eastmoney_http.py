# -*- coding: utf-8 -*-
"""
稳健版中文财经新闻抓取（仅用标准库）
- 先尝试 东方财富 搜索 API；若连续失败，自动回退 新浪公司新闻 页面抓取（HTML解析）
- 支持代理：--http_proxy / --https_proxy，或读取环境变量 HTTP_PROXY / HTTPS_PROXY
- 断点续抓：已有 data/news_multi/{TICKER}_news.csv 默认跳过（--force 可重抓）
- 可限页：--max_pages （建议先 5~10 页试跑）
输出：
  data/news_multi/{TICKER}_news.csv  (dt,ticker,title,source)
  data/news_all.csv  (汇总)
用法示例：
  python tools/fetch_news_eastmoney_http.py --max_pages 10 --sleep 0.2 --retries 5
  # 如需代理：
  python tools/fetch_news_eastmoney_http.py --https_proxy http://YOUR_PROXY:PORT --max_pages 10
"""
import argparse, csv, json, os, random, re, ssl, sys, time
from urllib.parse import urlencode
from urllib.request import Request, urlopen, build_opener, install_opener, ProxyHandler
from urllib.error import URLError, HTTPError
from pathlib import Path
from html import unescape

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DATA.mkdir(parents=True, exist_ok=True)

UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15",
]

def load_universe():
    pairs = []
    with open(DATA/"universe.csv", newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            pairs.append((row["ticker"], row.get("name","")))
    return pairs

def build_ctx_and_proxy(http_proxy: str, https_proxy: str):
    # 1) TLS：在受限环境可关闭校验（尽量仅在公司网使用）
    ctx = ssl._create_unverified_context()
    # 2) 代理：优先命令行，其次环境变量
    px = {}
    if http_proxy:  px["http"]  = http_proxy
    if https_proxy: px["https"] = https_proxy
    if not px:
        px = {k.lower(): v for k,v in os.environ.items() if k.upper() in ("HTTP_PROXY","HTTPS_PROXY")}
    if px:
        opener = build_opener(ProxyHandler(px))
        install_opener(opener)
        print(f"[INFO] Using proxy: {px}")
    return ctx

def http_get_json(url: str, ctx, timeout=20):
    req = Request(url, headers={"User-Agent": random.choice(UA_LIST), "Connection":"close"})
    with urlopen(req, timeout=timeout, context=ctx) as resp:
        return json.loads(resp.read().decode("utf-8", errors="ignore"))

def http_get_text(url: str, ctx, timeout=20):
    req = Request(url, headers={"User-Agent": random.choice(UA_LIST), "Connection":"close"})
    with urlopen(req, timeout=timeout, context=ctx) as resp:
        return resp.read().decode("utf-8", errors="ignore")

# ---------- 源1：东方财富搜索API ----------
def eastmoney_search(keyword: str, page: int, size: int, ctx):
    base = "https://search-api-web.eastmoney.com/search/jsonv2"
    payload = {"pageindex": page, "pagesize": size, "keyword": keyword, "type": [1]}
    url = f"{base}?{urlencode({'param': json.dumps(payload, ensure_ascii=False)})}"
    data = http_get_json(url, ctx)
    items = (data.get("Data") or {}).get("List") or []
    rows = []
    for it in items:
        title = it.get("Title") or it.get("title") or ""
        time_str = it.get("ShowTime") or it.get("showtime") or it.get("PublishTime") or ""
        src = it.get("MediaName") or it.get("mediaName") or "eastmoney"
        if not title or not time_str:
            continue
        dt = time_str.replace("T"," ").replace("Z","").split(".")[0]
        rows.append((dt, unescape(title), src))
    return rows

# ---------- 源2：新浪公司新闻（HTML页面） ----------
def sina_company_news_html(symbol: str, page: int, ctx):
    """
    新浪公司新闻页（分页），symbol: sz000001 / sh600000
    示例： https://vip.stock.finance.sina.com.cn/corp/view/vCB_AllNewsStock.php?symbol=sz000001&Page=1
    解析 <div class="datelist"> 下的 <li>，提取 [时间] 标题
    """
    base = "https://vip.stock.finance.sina.com.cn/corp/view/vCB_AllNewsStock.php"
    url = f"{base}?{urlencode({'symbol': symbol, 'Page': page})}"
    html = http_get_text(url, ctx)
    rows = []
    # 粗糙解析：时间在 [YYYY-MM-DD HH:MM] 形式，标题紧随其后
    for m in re.finditer(r"\[(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2})\]\s*<a[^>]*?>(.*?)</a>", html, flags=re.S|re.I):
        dt = m.group(1)
        title = unescape(re.sub(r"<.*?>","", m.group(2))).strip()
        if title:
            rows.append((dt, title, "sina"))
    return rows

def merge_dedupe(rows):
    seen = set(); out=[]
    for r in rows:
        k=(r[0], r[1])  # dt+title
        if k in seen: continue
        seen.add(k); out.append(r)
    return out

def save_csv(rows, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f); w.writerow(["dt","ticker","title","source"])
        for r in rows: w.writerow(r)

def fetch_one_ticker(ticker: str, name: str, max_pages: int, size: int, sleep: float, ctx, retries: int):
    all_rows=[]
    # 先东财：关键词= 代码(不带后缀) + 公司名
    for kw in [ticker.split(".")[0], name.strip()]:
        if not kw: continue
        for p in range(1, max_pages+1):
            attempt=0
            while True:
                try:
                    items = eastmoney_search(kw, p, size, ctx)
                    if not items: break
                    for (dt, title, src) in items:
                        all_rows.append([dt, ticker, title, src])
                    time.sleep(sleep)
                    break
                except (URLError, HTTPError, ssl.SSLError, OSError) as e:
                    attempt += 1
                    if attempt > retries:
                        print(f"[WARN] {ticker} EM kw={kw} p={p} 超过重试，转下一页。err={e}")
                        break
                    backoff = (2**(attempt-1))*(sleep+random.uniform(0, sleep))
                    print(f"[WARN] {ticker} EM kw={kw} p={p} 第{attempt}次失败：{e}，{backoff:.2f}s后重试")
                    time.sleep(backoff)

    # 若完全抓不到，再回退 新浪 HTML
    if not all_rows:
        ex = "sh" if ticker.endswith(".SS") else "sz"
        symbol = f"{ex}{ticker.split('.')[0]}"
        for p in range(1, max_pages+1):
            attempt=0
            while True:
                try:
                    items = sina_company_news_html(symbol, p, ctx)
                    if not items: break
                    for (dt, title, src) in items:
                        all_rows.append([dt, ticker, title, src])
                    time.sleep(sleep)
                    break
                except (URLError, HTTPError, ssl.SSLError, OSError) as e:
                    attempt += 1
                    if attempt > retries:
                        print(f"[WARN] {ticker} SINA p={p} 超过重试，转下一页。err={e}")
                        break
                    backoff = (2**(attempt-1))*(sleep+random.uniform(0, sleep))
                    print(f"[WARN] {ticker} SINA p={p} 第{attempt}次失败：{e}，{backoff:.2f}s后重试")
                    time.sleep(backoff)

    all_rows = merge_dedupe(all_rows)
    all_rows.sort(key=lambda x: x[0])
    return all_rows

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max_pages", type=int, default=50)
    ap.add_argument("--page_size", type=int, default=50)  # 仅对东财生效
    ap.add_argument("--sleep", type=float, default=0.3)
    ap.add_argument("--retries", type=int, default=3)
    ap.add_argument("--force", action="store_true", help="强制重抓该票（否则已存在文件跳过）")
    ap.add_argument("--http_proxy", type=str, default="")
    ap.add_argument("--https_proxy", type=str, default="")
    args = ap.parse_args()

    ctx = build_ctx_and_proxy(args.http_proxy, args.https_proxy)

    pairs = load_universe()
    news_dir = DATA/"news_multi"; news_dir.mkdir(parents=True, exist_ok=True)

    all_rows=[]
    for i,(tic,name) in enumerate(pairs,1):
        out_path = news_dir/f"{tic}_news.csv"
        if out_path.exists() and not args.force:
            print(f"[SKIP] {tic} 已存在，跳过（--force 可重抓）")
            with open(out_path, newline="", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    all_rows.append([row["dt"], row["ticker"], row["title"], row.get("source","eastmoney")])
            continue

        rows = fetch_one_ticker(tic, name, args.max_pages, args.page_size, args.sleep, ctx, args.retries)
        if rows:
            save_csv(rows, out_path)
            all_rows.extend(rows)
            print(f"[OK] {tic} 新闻数：{len(rows)}")
        else:
            print(f"[WARN] {tic} 无新闻（两源均失败或真无新闻）")

    if all_rows:
        all_rows.sort(key=lambda x: x[0])
        save_csv(all_rows, DATA/"news_all.csv")
        print(f"[OK] news_all.csv -> {len(all_rows)} 行")
    else:
        print("[WARN] 没抓到任何新闻。请尝试：--https_proxy http://IP:PORT 或换网络执行。")

if __name__ == "__main__":
    main()
