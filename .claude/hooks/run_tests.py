import json
import os
import subprocess
import sys

data = json.load(sys.stdin)
file_path = data.get("tool_input", {}).get("file_path", "")

in_tests = "/tests/" in file_path or ("tests" + os.sep) in file_path

if in_tests:
    subprocess.run(
        [sys.executable, "-m", "pytest", "tests/", "-q", "--no-header"],
        check=False,
    )
