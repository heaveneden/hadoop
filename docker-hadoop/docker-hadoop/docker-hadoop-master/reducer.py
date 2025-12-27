#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys

current_word = None
current_sum = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t", 1)
    if len(parts) != 2:
        continue

    word = parts[0]
    try:
        cnt = int(parts[1])
    except Exception:
        continue

    if current_word == word:
        current_sum += cnt
    else:
        if current_word is not None:
            sys.stdout.write("%s\t%d\n" % (current_word, current_sum))
        current_word = word
        current_sum = cnt

if current_word is not None:
    sys.stdout.write("%s\t%d\n" % (current_word, current_sum))
