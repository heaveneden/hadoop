# segment.py
# -*- coding: utf-8 -*-
"""
将 news_raw.txt (rank,title) -> news_seg.txt (rank,token1 token2 ...)
增强点：
1) 关闭 HMM 新词识别，减少“妹小”等怪词
2) 归一化（爆光->曝光 等）
3) 泛词/黑名单过滤 + 亲属称谓过滤
4) 轻量短语增强（相邻名词/专名合并），同时严格防止拼出垃圾词
"""

import re
import sys
from pathlib import Path

import jieba

try:
    import jieba.posseg as pseg
except Exception:
    pseg = None


BASE = Path(__file__).resolve().parent
RAW = BASE / "news_raw.txt"
OUT = BASE / "news_seg.txt"
STOP = BASE / "stopwords.txt"
USER_DICT = BASE / "user_dict.txt"


# 1) 常见写法归一化（你可以继续往里加）
NORMALIZE_MAP = {
    "爆光": "曝光",
    "官宣了": "官宣",
    "官宣啦": "官宣",
    "预告": "预告片",   # 你也可以反过来：把“预告片”统一成“预告”
}

# 2) 明确的垃圾词/泛词（可按你需要增删）
BAD_WORDS = set([
    "粉丝", "感动", "预告片", "回收", "产业链", "摔倒", "房型",
    "妹妹", "妹", "妹子", "小妹", "儿子", "女儿", "老公", "老婆",
    "姐姐", "姐", "哥哥", "哥", "妈妈", "妈", "爸爸", "爸",
    "小", "大", "太", "很", "真的", "觉得", "好像", "一个",
    "现场", "视频", "照片", "网友", "热搜", "回应", "最新",
    "爆料", "真相", "原因", "结果", "进展", "后续", "瓜",
    "妹小",  # 明确屏蔽你遇到的怪词
])

# 3) 亲属/称谓相关：只要 token 含这些字（且不是正规专名），直接过滤
KINSHIP_RE = re.compile(r"(妹|姐|哥|弟|嫂|姨|叔|舅|爸|妈|爹|娘|儿子|女儿|老公|老婆)")

# 4) 允许保留的词性（偏“热点实体/事件”）
# nr:人名 ns:地名 nt:机构名 nz:其他专名 n*:名词 v*:动词
ALLOW_POS_PREFIX = ("n", "v")
ALLOW_POS_EXACT = set(["nr", "ns", "nt", "nz"])

# 5) 用于短语合并时禁止出现的“组件词”（防止拼出“妹小/小X”这种）
BAD_COMPONENT = set(["妹", "小", "姐", "哥", "儿子", "女儿", "粉丝"])


def load_stopwords(path):
    if not path.exists():
        return set()
    txt = path.read_text(encoding="utf-8", errors="ignore")
    return set([w.strip() for w in txt.splitlines() if w.strip()])


def load_user_dict(path):
    if path.exists():
        try:
            jieba.load_userdict(str(path))
        except Exception:
            pass


def clean_title(s):
    """
    清洗标题：保留中英文数字，其他符号统一空格，避免粘连造词
    """
    # 注意：这里用空格隔开，减少“字黏在一起”触发新词
    s = re.sub(r"[^\u4e00-\u9fffA-Za-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalize_word(w):
    w = w.strip()
    if not w:
        return ""
    # 统一大小写（英文）
    if re.fullmatch(r"[A-Za-z]+", w):
        w = w.upper()
    # 映射归一化
    if w in NORMALIZE_MAP:
        w = NORMALIZE_MAP[w]
    return w


def is_bad_token(w, stop_set):
    if not w:
        return True
    if w in stop_set:
        return True
    if w in BAD_WORDS:
        return True

    # 纯数字、太短（中文 1 字通常信息太弱）
    if w.isdigit():
        return True

    # 英文太短（如 "a"）
    if re.fullmatch(r"[A-Za-z]+", w) and len(w) < 2:
        return True

    # 中文长度限制：一般 2~8 比较合理；太长多半是句子碎片
    if re.search(r"[\u4e00-\u9fff]", w):
        if len(w) < 2:
            return True
        if len(w) > 10:
            return True

    # 含亲属/称谓的碎词直接丢（保守一点：长度<=4 更像碎片）
    if KINSHIP_RE.search(w) and len(w) <= 4:
        return True

    return False


def allow_by_pos(flag):
    if not flag:
        return False
    if flag in ALLOW_POS_EXACT:
        return True
    for p in ALLOW_POS_PREFIX:
        if flag.startswith(p):
            return True
    return False


def tokenize(title, stop_set):
    """
    分词 + 过滤 + 轻量短语增强
    返回：tokens（按出现顺序，去重）
    """
    title = clean_title(title)
    if not title:
        return []

    tokens = []
    pos_list = []

    # 关闭 HMM：减少怪词
    if pseg is not None:
        for w, flag in pseg.cut(title, HMM=False):
            w = normalize_word(w)
            if not w:
                continue
            if not allow_by_pos(flag):
                continue
            if is_bad_token(w, stop_set):
                continue
            tokens.append(w)
            pos_list.append((w, flag))
    else:
        # 没有 posseg 就退化到普通分词（也关 HMM）
        for w in jieba.lcut(title, HMM=False):
            w = normalize_word(w)
            if is_bad_token(w, stop_set):
                continue
            tokens.append(w)
            pos_list.append((w, ""))

    # —— 短语增强：相邻“专名/名词”尝试合并为短语 —— #
    # 目的：把被拆开的实体拼回去；但严禁出现 BAD_COMPONENT
    phrases = []
    for i in range(len(pos_list) - 1):
        w1, f1 = pos_list[i]
        w2, f2 = pos_list[i + 1]

        # 组件黑名单（防止“妹小/小X”）
        if w1 in BAD_COMPONENT or w2 in BAD_COMPONENT:
            continue
        if KINSHIP_RE.search(w1) or KINSHIP_RE.search(w2):
            continue

        # 两个都必须是名词/专名类，才合并
        ok1 = (f1 in ALLOW_POS_EXACT) or (f1.startswith("n")) or (f1 == "")
        ok2 = (f2 in ALLOW_POS_EXACT) or (f2.startswith("n")) or (f2 == "")
        if not (ok1 and ok2):
            continue

        phrase = w1 + w2
        phrase = normalize_word(phrase)

        # 合并短语再做一次过滤
        if is_bad_token(phrase, stop_set):
            continue
        # 合并短语长度控制：2~8 更像实体/话题
        if len(phrase) < 2 or len(phrase) > 8:
            continue

        phrases.append(phrase)

    # 合并：短语优先放前面（更像“热点短语”）
    merged = phrases + tokens

    # 去重但保留顺序
    seen = set()
    final_tokens = []
    for w in merged:
        if w not in seen:
            seen.add(w)
            final_tokens.append(w)

    # 每条最多保留一定数量，避免 mapper 输入过长
    return final_tokens[:40]


def main():
    if not RAW.exists():
        sys.stderr.write("[segment.py] 找不到输入文件：%s\n" % str(RAW))
        sys.exit(1)

    load_user_dict(USER_DICT)
    stop_set = load_stopwords(STOP)

    lines = RAW.read_text(encoding="utf-8", errors="ignore").splitlines()
    out_lines = []

    for line in lines:
        line = line.strip()
        if not line or "," not in line:
            continue

        rank_str, title = line.split(",", 1)
        rank_str = rank_str.strip()
        title = title.strip()
        if not rank_str.isdigit():
            continue

        toks = tokenize(title, stop_set)
        if not toks:
            continue

        out_lines.append("%s,%s" % (rank_str, " ".join(toks)))

    if not out_lines:
        sys.stderr.write("[segment.py] 分词输出为空：请检查 news_raw.txt 格式/内容\n")
        sys.exit(1)

    OUT.write_text("\n".join(out_lines), encoding="utf-8", errors="ignore")
    sys.stdout.write("分词完成：输出 %d 行 -> %s\n" % (len(out_lines), str(OUT)))


if __name__ == "__main__":
    main()
