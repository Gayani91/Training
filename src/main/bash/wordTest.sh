#!/usr/bin/env bash
for word in $(cat data.text);

    do

    echo "$word: $(grep -c $word data.text)";

done | sort -u