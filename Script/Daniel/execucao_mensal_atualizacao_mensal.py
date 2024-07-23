import subprocess
import sys
import time
import os

files = [
    'g8.1--g8.2.py', 'g18.5.py', 'g18.6.py', 't18.1.py', 't18.2.py'
]

for f in files:
    subprocess.run([sys.executable, os.path.join('Scripts/Daniel', f)])
    time.sleep(3)
