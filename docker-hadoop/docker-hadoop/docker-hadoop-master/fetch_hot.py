# fetch_hot.py
import sys
import time
from pathlib import Path

import requests

HOT_API = "https://weibo.com/ajax/side/hotSearch"
OUT_FILE = Path(__file__).resolve().parent / "news_raw.txt"


def save_hot(titles: list[str]) -> None:
    # 覆盖写入：排名,标题
    with OUT_FILE.open("w", encoding="utf-8") as f:
        for i, title in enumerate(titles, start=1):
            f.write(f"{i},{title}\n")

    print(f"抓取成功：已保存 {len(titles)} 条热搜 -> {OUT_FILE}")


def main() -> None:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Referer": "https://weibo.com/hot/search",
    }

    try:
        print("正在抓取微博实时热搜...")
        resp = requests.get(HOT_API, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        realtime = data.get("data", {}).get("realtime", [])
        titles: list[str] = []
        for item in realtime:
            if item.get("is_ad"):
                continue
            w = (item.get("word") or "").strip()
            if w:
                titles.append(w)

        if not titles:
            raise RuntimeError("接口返回为空：titles 为空（可能被限流/结构变化）")

        save_hot(titles)
        print("更新时间：", time.strftime("%Y-%m-%d %H:%M:%S"))

    except Exception as e:
        print(f"[fetch_hot.py] 抓取失败：{e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
