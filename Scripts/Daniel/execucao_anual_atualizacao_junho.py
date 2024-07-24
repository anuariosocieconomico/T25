import subprocess
import sys
import time
import os

files = [
    'g7.1--g7.2--t7.1.py'
]

for f in files:
    subprocess.run([sys.executable, os.path.join('Scripts/Daniel', f)])
    time.sleep(3)
