import subprocess
import sys
import time
import os

files = [
    'g11.11.py'
]

for f in files:
    subprocess.run([sys.executable, os.path.join('Scripts/Daniel', f)])
    time.sleep(3)
