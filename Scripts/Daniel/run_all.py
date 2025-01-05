import subprocess
import sys
import time
import os

files = [
    'g1.5--g1.6--g1.7--g1.8--t1.1.py', 'g11.11.py', 'g11.2.py', 'g13.6.py', 'g17.1.py', 'g17.2.py',
    'g18.4.py', 'g18.5.py', 'g18.6.py', 'g18.7.py', 'g18.8.py', 'g19.1--g19.3--g19.7--g19.9.py',
    'g19.10--g19.11--g19.12.py', 'g19.14.py', 'g19.2--g19.4--g19.5--g19.6.py', 'g19.8.py', 'g20.1--g20.2.py',
    'g20.11--g20.12.py', 'g20.3--g20.4.py', 'g4.2.py', 'g5.4.py', 'g7.1--g7.2--t7.1.py', 'g8.1--g8.2.py', 't13.2.py',
    't17.1.py', 't17.2.py', 't18.1.py', 't18.2.py'
]

print(len(files))

for f in files:
    subprocess.run([sys.executable, f])
    time.sleep(3)
