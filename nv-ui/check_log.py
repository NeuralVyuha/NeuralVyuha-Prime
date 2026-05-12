
import re
import os

# Use relative path from current directory
log_file = os.path.join(os.path.dirname(__file__), 'ngtemplates_log.txt')
with open(log_file, 'r', encoding='utf-16le') as f:
    lines = f.readlines()

for i, line in enumerate(lines):
    if 'views/partials/case/case.details.html' in line:
        print(f"Line {i}: {line.strip()}")
        # Print context
        for j in range(max(0, i-2), min(len(lines), i+3)):
            if j != i:
                print(f"  [{j}] {lines[j].strip()}")
