import os
import json
import glob

DOCS_DIR = "docs"
MANUAL_PATH = "docs/USER_MANUAL.md"
OUTPUT_PATH = "frontend/app/assets/manual.json" # If we want to serve it

def generate_manual():
    print("Generating Unified User Manual...")

    manual_content = "# NeuralVyuha User Manual\n\n"

    # 1. Introduction (from INSTALLATION.md snippet or custom)
    manual_content += "## 1. Introduction\n"
    manual_content += "NeuralVyuha is the next-gen SOC engine. This manual covers all aspects of daily operations.\n\n"

    # 2. Installation
    if os.path.exists("docs/INSTALLATION.md"):
        with open("docs/INSTALLATION.md", "r") as f:
            manual_content += f.read() + "\n\n"

    # 3. Architecture
    if os.path.exists("docs/ARCHITECTURE.md"):
        with open("docs/ARCHITECTURE.md", "r") as f:
            manual_content += f.read() + "\n\n"

    # 4. API Reference
    if os.path.exists("docs/API_REFERENCE.md"):
        with open("docs/API_REFERENCE.md", "r") as f:
            manual_content += f.read() + "\n\n"

    # 5. Append all ADRs
    manual_content += "## 5. Architectural Decisions (ADRs)\n"
    adrs = sorted(glob.glob("docs/v5-internal/ADR-*.md"))
    for adr in adrs:
        with open(adr, "r") as f:
            manual_content += f"### {os.path.basename(adr)}\n"
            manual_content += f.read() + "\n\n"

    # Write full MD
    with open(MANUAL_PATH, "w") as f:
        f.write(manual_content)
    print(f"Manual generated at {MANUAL_PATH}")

    # (Optional) Convert to JSON for UI consumption if needed
    # json_manual = {"content": manual_content}
    # with open(OUTPUT_PATH, "w") as f:
    #     json.dump(json_manual, f)

if __name__ == "__main__":
    generate_manual()
