import os, csv, sys, time, traceback
import requests, feedparser, yaml
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime

# 从 src.utils 引入工具函数
from src.utils import (
    norm_text,
    parse_date,
    looks_like_article_link,
    contains_keywords,
    contains_borough,
    to_iso
)

ROOT = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(ROOT, "data")
OUT_DIR = os.path.join(DATA_DIR, "output")
SEEN_PATH = os.path.join(DATA_DIR, "seen_urls.csv")
EXCEL_PATH = os.path.join(OUT_DIR, "queens_dev_news.xlsx")

os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

# 读取配置
with open(os.path.join(ROOT, "src", "sources.yaml"), "r", encoding="utf-8") as f:
    SOURCES = yaml.safe_load(f)
with open(os.path.join(ROOT, "src", "keywords.yaml"), "r", encoding="utf-8") as f:
    KW = yaml.safe_load(f)

BOROUGHS = KW.get("boroughs", [])
MUST_ANY = KW.get("must_have_any", [])

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; QueensDevNewsBot/1.0)"
}

def load_seen():
    seen = set()
    if os.path.exists(SEEN_PATH):
        with open(SEEN_PATH, "r", newline="", encoding="utf-8") as f:
            for row in csv.reader(f):
                if row:
                    seen.add(row[0])
    return seen

def save_seen(seen):
    with open(SEEN_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        for u in sorted(seen):
            w.writerow([u])

def fetch_url(url, timeout=20):
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r

def parse_rss(feed_url):
    fp = feedparser.parse(feed_url)
    items = []
    for e in fp.entries:
        title = norm_text(e.get("title"))
        link = e.get("link")
        summary = norm_text(e.get("summary") or e.get("description"))
        dt = None
        for k in ("published", "updated"):
            if e.get(k):
                dt = parse_date(e.get(k))
                if dt: break
        items.append({
            "title": title, "url": link, "summary": summary,
            "published": dt, "raw_date": e.get("published") or e.get("updated")
        })
    return items

def parse_html_list(page_url, list_sel, title_sel, link_sel, date_sel, summary_sel):
    r = fetch_url(page_url)
    soup = BeautifulSoup(r.text, "html.parser")
    blocks = soup.select(list_sel) if list_sel else soup.find_all("article")
    items = []
    for b in blocks:
        # title
        title_el = None
        for sel in (title_sel or "").split(","):
            sel = sel.strip()
            if not sel: continue
            title_el = b.select_one(sel)
            if title_el: break
        if not title_el:
            continue
        title = norm_text(title_el.get_text())

        # link
        link_el = b.select_one(link_sel) if link_sel else title_el
        href = link_el.get("href") if link_el else None
        if href and not href.startswith("http"):
            href = urljoin(page_url, href)
        if not looks_like_article_link(href):
            continue

        # date
        dt_txt = None
        if date_sel:
            d_el = b.select_one(date_sel)
            if d_el:
                dt_txt = norm_text(d_el.get("datetime") or d_el.get_text())
        dt = parse_date(dt_txt)

        # summary
        summ = None
        if summary_sel:
            s_el = b.select_one(summary_sel)
            if s_el:
                summ = norm_text(s_el.get_text())

        items.append({
            "title": title, "url": href, "summary": summ, "published": dt, "raw_date": dt_txt
        })
    return items

def enrich_article(article):
    """补抓正文首段，提升关键词匹配成功率（轻量请求）。"""
    try:
        r = fetch_url(article["url"])
        soup = BeautifulSoup(r.text, "html.parser")
        body = soup.select_one("article") or soup.select_one(".entry-content") or soup
        paras = body.find_all(["p", "h2", "li"], limit=6)
        extra = " ".join(norm_text(p.get_text()) for p in paras if p)
        article["content_preview"] = extra[:1200]
    except Exception:
        article["content_preview"] = None
    return article

def filter_items(items):
    """
    放宽规则：
    - YIMBY 的 Queens 子频道（feed 名字包含 Queens 各社区）只需命中关键词即可；
    - 其他源仍需 地名 + 关键词。
    """
    QUEENS_FEED_HINTS = {
        "long island city", "lic", "astoria", "flushing", "jamaica",
        "ridgewood", "sunnyside", "woodside", "rego park", "forest hills",
        "kew gardens", "bayside", "whitestone", "college point",
        "maspeth", "elmhurst", "jackson heights", "corona",
        "rockaway", "far rockaway", "howard beach", "middle village", "ozone park"
    }

    filtered = []
    for it in items:
        text_blob = " ".join([
            it.get("title") or "",
            it.get("summary") or "",
            it.get("content_preview") or ""
        ]).lower()

        source = (it.get("source") or "").lower()
        feed_name = (it.get("feed_name") or "").lower()

        is_yimby_queens_feed = (
            source == "yimby" and any(h in feed_name for h in QUEENS_FEED_HINTS)
        )

        has_borough = contains_borough(text_blob, BOROUGHS)
        has_keyword = contains_keywords(text_blob, MUST_ANY)

        if is_yimby_queens_feed:
            if has_keyword:
                filtered.append(it)
        else:
            if has_borough and has_keyword:
                filtered.append(it)
    return filtered

def main():
    seen = load_seen()
    rows = []  # 保证 rows 在 main() 顶部定义

    # --- RSS 源 ---
    for src in SOURCES.get("rss_sources", []):
        try:
            items = parse_rss(src["url"])
            for it in items[:80]:
                it["source"] = src["source"]
                it["feed_name"] = src["name"]
                if it["url"] in seen:
                    continue
                it = enrich_article(it)
                if filter_items([it]):
                    seen.add(it["url"])
                    rows.append(it)
            print(f"[RSS DONE] {src['name']} -> kept so far: {len(rows)}")
            time.sleep(1.0)
        except Exception:
            print(f"[RSS ERROR] {src['name']}: {traceback.format_exc()}", file=sys.stderr)

    # --- HTML 源 ---
    for src in SOURCES.get("html_sources", []):
        try:
            items = parse_html_list(
                src["url"], src.get("list_selector"), src.get("title_selector"),
                src.get("link_selector"), src.get("date_selector"), src.get("summary_selector")
            )
            for it in items[:80]:
                it["source"] = src["source"]
                it["feed_name"] = src["name"]
                if it["url"] in seen:
                    continue
                it = enrich_article(it)
                if filter_items([it]):
                    seen.add(it["url"])
                    rows.append(it)
            print(f"[HTML DONE] {src['name']} -> kept so far: {len(rows)}")
            time.sleep(1.0)
        except Exception:
            print(f"[HTML ERROR] {src['name']}: {traceback.format_exc()}", file=sys.stderr)

    print(f"[TOTAL] kept rows: {len(rows)}")

    # --- 汇总输出 ---
    if rows:
        df = pd.DataFrame(rows)
    else:
        df = pd.DataFrame(columns=[
            "title","url","summary","published","raw_date","source","feed_name","content_preview"
        ])

    # 最近 7 天
    now = datetime.now()
    def fresh(d):
        if d is None or pd.isna(d):
            return True
        try:
            return (pd.to_datetime(d) >= now - pd.Timedelta(days=7))
        except Exception:
            return True

    if "published" in df.columns and not df.empty:
        df = df[df["published"].apply(fresh)]
    if "published" in df.columns:
        df = df.sort_values(by="published", ascending=False, na_position="last")

    os.makedirs(OUT_DIR, exist_ok=True)
    df_out = df.copy()
    df_out.rename(columns={
        "title": "Title",
        "url": "URL",
        "summary": "Summary",
        "published": "Published",
        "source": "Source",
        "feed_name": "Feed"
    }, inplace=True)
    df_out["Published"] = df_out["Published"].apply(lambda x: to_iso(x))
    df_out.to_excel(EXCEL_PATH, index=False)

    save_seen(seen)
    print(f"Saved {len(df_out)} rows to {EXCEL_PATH}")

if __name__ == "__main__":
    main()

