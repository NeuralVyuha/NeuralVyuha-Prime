import os

APP_JS = "frontend/app/scripts/app.js"
INSERT_MARKER = ".state('app.alert-list', {"

NEW_ROUTES = """            .state('app.nv-help', {
                url: '/nv/help',
                templateUrl: 'views/nv/help.html',
                controller: 'NvHelpCtrl',
                controllerAs: 'vm',
                guard: {
                    isSuperAdmin: false
                }
            })
"""

def main():
    try:
        with open(APP_JS, 'r') as f:
            content = f.read()
    except FileNotFoundError:
        print(f"File {APP_JS} not found.")
        return

    if "app.nv-help" in content:
        print("Help route already exists.")
        return

    if INSERT_MARKER not in content:
        print("Could not find insert marker.")
        return

    parts = content.split(INSERT_MARKER)

    new_content = parts[0] + NEW_ROUTES + INSERT_MARKER + parts[1]

    with open(APP_JS, 'w') as f:
        f.write(new_content)
    print("Help route injected.")

if __name__ == "__main__":
    main()
