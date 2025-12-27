#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys

# 输入：rank,word1 word2...
# 输出：word \t weight
for line in sys.stdin:
    line = line.strip()
    if not line or "," not in line:
        continue

    try:
        rank_str, words_str = line.split(",", 1)
        rank = int(rank_str)
        weight = 51 - rank  # 第1名50分，第50名1分
        if weight < 1:
            weight = 1
    except Exception:
        weight = 1
        words_str = line.split(",", 1)[-1]

    for w in words_str.split():
        if w:
            sys.stdout.write("%s\t%d\n" % (w, weight))
