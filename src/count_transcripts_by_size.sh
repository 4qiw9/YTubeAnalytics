#!/bin/bash

BASE_DIR="../output/transcripts/"

echo "Number of files:"
echo "---------------------------"

count_lt_1kb=0
count_1_10kb=0
count_10_20kb=0
count_20_50kb=0
count_50_100kb=0
count_gt_100kb=0

for dir in "$BASE_DIR"*/; do
    if [ -d "$dir" ]; then
        file_count=$(find "$dir" -type f | wc -l)
        echo "Dir: $(basename "$dir") - number of files: $file_count"

        while IFS= read -r file; do
            size=$(stat -f "%z" "$file")
            if [ "$size" -lt 1024 ]; then
                ((count_lt_1kb++))
            elif [ "$size" -ge 1024 ] && [ "$size" -lt 10240 ]; then
                ((count_1_10kb++))
            elif [ "$size" -ge 10240 ] && [ "$size" -lt 20480 ]; then
                ((count_10_20kb++))
            elif [ "$size" -ge 20480 ] && [ "$size" -lt 51200 ]; then
                ((count_20_50kb++))
            elif [ "$size" -ge 51200 ] && [ "$size" -lt 102400 ]; then
                ((count_50_100kb++))
            else
                ((count_gt_100kb++))
            fi
        done < <(find "$dir" -type f)
    fi
done

echo ""
echo "Summary by size:"
echo "---------------------------"
echo "< 1 KB: $count_lt_1kb"
echo "1 KB - 10 KB: $count_1_10kb"
echo "10 KB - 20 KB: $count_10_20kb"
echo "20 KB - 50 KB: $count_20_50kb"
echo "50 KB - 100 KB: $count_50_100kb"
echo "> 100 KB: $count_gt_100kb"
