
import os

# Use relative path from current directory  
bundle_path = os.path.join(os.path.dirname(__file__), 'dist', 'scripts', 'scripts.9dd301c1.js')
with open(bundle_path, 'r', encoding='utf-8') as f:
    bundle = f.read()

pattern = "NvRouter"
start_search = 0
while True:
    idx = bundle.find(pattern, start_search)
    if idx == -1: break
    print(f"\n--- HIT AT {idx} ---")
    print(bundle[idx-100:idx+200])
    start_search = idx + len(pattern)
