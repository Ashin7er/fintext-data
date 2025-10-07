# -*- coding: utf-8 -*-
"""
多股票新闻+价格对齐，生成 labels_joined.csv（供训练/评估）
Usage:
  python tools/ingest_multi_tickers.py \
    --news_dir data/news_multi \
    --prices_dir data/prices_multi \
    --out_labels data/labels_joined.csv \
    --horizon 1 \
    --session_close "15:00" \
    --delay_minutes 30
"""
import argparse
import numpy as np
import pandas as pd
from pathlib import Path
from pandas.tseries.offsets import BDay
from datetime import time as dtime

def effective_timestamp(ts, session_close="15:00"):
    hh, mm = map(int, session_close.split(':'))
    ts = pd.Timestamp(ts)
    return (ts + BDay(1)).normalize() if ts.time() > dtime(hh, mm) else ts.normalize()

def load_concat_csvs(folder: Path, kind="news"):
    rows = []
    for fp in sorted(folder.glob("*.csv")):
        df = pd.read_csv(fp)
        if kind=="news":
            need = {"dt","title"}
            if not need.issubset(df.columns): raise ValueError(f"{fp} 缺列 {need}")
            if "ticker" not in df.columns: df["ticker"] = fp.stem.split("_")[0]
            df["dt"] = pd.to_datetime(df["dt"])
            rows.append(df[["dt","ticker","title"]])
        else:
            need = {"dt","close"}
            if not need.issubset(df.columns): raise ValueError(f"{fp} 缺列 {need}")
            if "ticker" not in df.columns: df["ticker"] = fp.stem.split("_")[0]
            df["dt"] = pd.to_datetime(df["dt"])
            keep = ["dt","ticker","open","high","low","close","volume"]
            rows.append(df[[c for c in keep if c in df.columns]])
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--news_dir", type=str, default="data/news_multi")
    ap.add_argument("--prices_dir", type=str, default="data/prices_multi")
    ap.add_argument("--out_labels", type=str, default="data/labels_joined.csv")
    ap.add_argument("--horizon", type=int, default=1)
    ap.add_argument("--session_close", type=str, default="15:00")
    ap.add_argument("--delay_minutes", type=int, default=30)
    args = ap.parse_args()

    news = load_concat_csvs(Path(args.news_dir), "news")
    prices = load_concat_csvs(Path(args.prices_dir), "prices")
    if news.empty or prices.empty:
        raise SystemExit("news 或 prices 为空，请先执行抓取脚本")

    news["eff_date"] = news["dt"].apply(lambda x: effective_timestamp(x, args.session_close))
    prices["date"] = pd.to_datetime(prices["dt"]).dt.normalize()

    have = prices[["ticker","date"]].drop_duplicates().rename(columns={"date":"eff_date"})
    news = news.merge(have, on=["ticker","eff_date"], how="inner")

    px = prices.sort_values(["ticker","dt"]).copy()
    px["ret_fwd"] = px.groupby("ticker")["close"].pct_change(periods=args.horizon).shift(-args.horizon)

    feats = news[["dt","ticker","title"]].copy()
    feats["t0"] = feats["dt"] + pd.Timedelta(minutes=args.delay_minutes)
    aligned = pd.merge_asof(
        feats.sort_values(["ticker","t0"]),
        px.sort_values(["ticker","dt"]),
        by="ticker", left_on="t0", right_on="dt", direction="forward"
    ).dropna(subset=["ret_fwd"])

    aligned["y"] = (aligned["ret_fwd"] > 0).astype(int)
    out = aligned.rename(columns={"ret_fwd":"ret"})[["title","y","ret","ticker","t0"]]
    out.to_csv(args.out_labels, index=False, encoding="utf-8-sig")
    print(f"[OK] labels saved -> {args.out_labels} , rows={len(out)}")

if __name__ == "__main__":
    main()
