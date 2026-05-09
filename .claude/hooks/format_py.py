import json
import subprocess
import sys

data = json.load(sys.stdin)
file_path = data.get("tool_input", {}).get("file_path", "")

if file_path.endswith(".py") and ".venv" not in file_path:
    subprocess.run([sys.executable, "-m", "black", file_path], check=False)
    subprocess.run([sys.executable, "-m", "isort", file_path], check=False)
