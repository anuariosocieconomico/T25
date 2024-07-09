import subprocess
import sys
import time

files = [
    'g19.14.py'
]

for f in files:
    subprocess.run([sys.executable, os.path.join('Scripts/Daniel', f)])
    time.sleep(3)
