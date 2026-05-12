import os

INDEX_HTML = "frontend/app/index.html"
MARKER = '<script src="scripts/services/nvApiSrv.js"></script>'
NEW_SCRIPT = '    <script src="scripts/controllers/nv/nvHelpCtrl.js"></script>'

def main():
    try:
        with open(INDEX_HTML, 'r') as f:
            content = f.read()
    except FileNotFoundError:
        print(f"File {INDEX_HTML} not found.")
        return

    if "nvHelpCtrl.js" in content:
        print("Help script already exists.")
        return

    if MARKER not in content:
        # Try finding another marker if first fails
        MARKER_ALT = '<script src="scripts/controllers/nv/nvGroupDetailController.js"></script>'
        if MARKER_ALT in content:
             content = content.replace(MARKER_ALT, MARKER_ALT + "\n" + NEW_SCRIPT)
        else:
             print("Could not find insert marker.")
             return
    else:
        content = content.replace(MARKER, MARKER + "\n" + NEW_SCRIPT)

    with open(INDEX_HTML, 'w') as f:
        f.write(content)
    print("Help script injected.")

if __name__ == "__main__":
    main()
