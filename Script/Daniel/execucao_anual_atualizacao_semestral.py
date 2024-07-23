import subprocess
import sys
import time
import os

'''
NO SCRIPT "g20.11--g20.12.py" APENAS A PLANILHA G19.12 SERÁ ELABORADA, POIS A OUTRA JÁ FOI CONFIGURADA POR RODRIGO.
NO SCRIPT "g20.1--g20.2.py" APENAS A PLANILHA G19.2 SERÁ ELABORADA, POIS A OUTRA JÁ FOI CONFIGURADA POR RODRIGO.
NO SCRIPT "g20.3--g20.4.py" APENAS A PLANILHA G19.4 SERÁ ELABORADA, POIS A OUTRA JÁ FOI CONFIGURADA POR RODRIGO.

AO TODO SERÃO ELABORADAS 38 PLANILHAS.
'''

files = [
    'g1.5--g1.6--g1.7--g1.8--t1.1.py', 'g4.2.py', 'g5.4.py', 'g11.2.py', 'g17.1.py', 'g17.2.py', 'g18.4.py', 'g18.7.py',
    'g18.8.py', 'g19.2--g19.4--g19.5--g19.6.py', 'g20.1--g20.2.py', 'g20.3--g20.4.py', 'g20.11--g20.12.py'
]

for f in files:
    subprocess.run([sys.executable, os.path.join('Scripts/Daniel', f)])
    time.sleep(3)
