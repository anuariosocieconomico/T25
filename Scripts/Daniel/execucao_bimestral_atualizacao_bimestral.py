import subprocess
import sys
import time
import os

files = [
    'g19.14.py'
]

for f in files:
    subprocess.run([sys.executable, f])
    time.sleep(3)
