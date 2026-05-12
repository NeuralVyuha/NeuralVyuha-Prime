
import os

# Use relative path from current directory
bundle_path = os.path.join(os.path.dirname(__file__), 'dist', 'scripts', 'scripts.9dd301c1.js')
with open(bundle_path, 'r', encoding='utf-8') as f:
    bundle = f.read()

pattern = "Basic Information"
idx = bundle.find(pattern)
if idx >= 0:
    start = max(0, idx - 500)
    end = min(len(bundle), idx + 1000)
    print(f"--- HIT AT {idx} ---")
    print(bundle[start:end])
else:
    print("Pattern not found")

# Also find all occurrences of the details template string to see their content
pattern2 = "views/partials/case/case.details.html"
start_search = 0
while True:
    idx = bundle.find(pattern2, start_search)
    if idx == -1: break
    print(f"\n--- TEMPLATE STRING AT {idx} ---")
    print(bundle[idx-50:idx+200])
    start_search = idx + len(pattern2)
