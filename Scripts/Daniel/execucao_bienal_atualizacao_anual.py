import subprocess
import sys
import time
import os

files = [
    't17.1.py', 't17.2.py'
]

for f in files:
    subprocess.run([sys.executable, f])
    time.sleep(3)
