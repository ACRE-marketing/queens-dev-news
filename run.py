# run.py
import os
import pandas as pd
from datetime import timedelta

# 尝试导入三个子模块，如果不存在也不影响
try:
    from src import pincusco, yimby, cityrealty
except Exception as e:
    pincusco = yimby = cityrealty = None
    print(f"[WARN] import sources failed: {e}")

# ---------------- 配置 ----------------
OUTPUT_XLSX = "data/output/queens_dev_news.xlsx"
DAILY_SHEET = "daily_log"
WEEKLY_SHEET = "weekly_rollup"
DAILY_WINDOW = timedelta(days=2)   # 日更时间窗口：48小时
WEEKLY_WINDOW = timedelta(days=7)  # 周更时间窗口：7天

# ---------------- 工具函数 ----------------
def _load_sheet(path, sheet):
    """加载已有的 sheet，如果不存在就返回 None"""
    if not os.path.exists(path):
        return None
    try:
        return pd.read_excel(path, sheet_name=sheet, engine="openpyxl")
    except Exception:
        return None

def _save_excel(path, daily_df, weekly_df):
    """保存到 Excel，即使是空 DataFrame 也会写入"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl", mode="w") as w:
        (daily_df if daily_df is not None else
         pd.DataFrame(columns=["date","title","neighborhood","action","source","link"])
        ).to_excel(w, index=False, sheet_name=DAILY_SHEET)

        (weekly_df if weekly_df is not None else
         pd.DataFrame(columns=["date","title","neighborhood","action","source","link"])
        ).to_excel(w, index=False, sheet_name=WEEKLY_SHEET)

def _fresh_filter(df, window_td):
    """按时间窗口筛选数据"""
    if df is None or df.empty:
        return pd.DataFrame(columns=["date","title","neighborhood","action","source","link"])
    df = df.copy()
    df["date_parsed"] = pd.to_datetime(df["date"], errors="coerce", utc=True)
    cutoff = pd.Timestamp.utcnow() - pd.Timedelta(seconds=window_td.total_seconds())
    return df[df["date_parsed"] >= cutoff].drop(columns=["date_parsed"])

def _ensure_cols(df):
    """确保 DataFrame 包含固定列"""
    cols = ["date","title","neighborhood","action","source","link"]
    if df is None:
        return pd.DataFrame(columns=cols)
    for c in cols:
        if c not in df.columns:
            df[c] = None
    return df[cols]

def _dedupe(df_new, df_old):
    """去重：根据标题和链接"""
    if df_old is None or df_old.empty:
        return df_new
    cat = pd.concat([df_old, df_new], ignore_index=True)
    cat = cat.drop_duplicates(subset=["title","link"], keep="first")
    return cat

# ---------------- 抓取逻辑 ----------------
def crawl_all_safe():
    rows = []
    if pincusco:
        try:
            rows.extend(pincusco.fetch_recent())
        except Exception as e:
            print(f"[WARN] PincusCo fetch failed: {e}")
    if yimby:
        try:
            rows.extend(yimby.fetch_recent())
        except Exception as e:
            print(f"[WARN] YIMBY fetch failed: {e}")
    if cityrealty:
        try:
            rows.extend(cityrealty.fetch_recent())
        except Exception as e:
            print(f"[WARN] CityRealty fetch failed: {e}")

    df = pd.DataFrame(rows, columns=["date","title","neighborhood","action","source","link"])
    df = _ensure_cols(df)
    return df

def main(mode="daily"):
    print(f"[INFO] Run mode = {mode}")
    new_df = crawl_all_safe()
    print(f"[INFO] Crawled rows = {len(new_df)}")

    daily_old = _load_sheet(OUTPUT_XLSX, DAILY_SHEET)
    weekly_old = _load_sheet(OUTPUT_XLSX, WEEKLY_SHEET)

    daily_df = _fresh_filter(new_df, DAILY_WINDOW)
    weekly_df = _fresh_filter(new_df, WEEKLY_WINDOW)

    daily_all = _dedupe(daily_df, daily_old)
    weekly_all = _dedupe(weekly_df, weekly_old)

    _save_excel(OUTPUT_XLSX, daily_all, weekly_all)

    print(f"[INFO] Saved Excel at {OUTPUT_XLSX}")
    print(f"[INFO] daily: new_in_window={len(daily_df)}, total={len(daily_all)}")
    print(f"[INFO] weekly: new_in_window={len(weekly_df)}, total={len(weekly_all)}")

# ---------------- 程序入口 ----------------
if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "daily"
    try:
        main(mode)
    except Exception as e:
        print(f"[FATAL] run failed but will still write empty Excel: {e}")
        # 出错时也写出空 Excel，保证文件存在
        _save_excel(OUTPUT_XLSX,
                    pd.DataFrame(columns=["date","title","neighborhood","action","source","link"]),
                    pd.DataFrame(columns=["date","title","neighborhood","action","source","link"]))
        print(f"[INFO] Wrote empty Excel to {OUTPUT_XLSX}")

