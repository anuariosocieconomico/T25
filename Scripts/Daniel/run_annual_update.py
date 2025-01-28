import os
import subprocess
import sys
from time import sleep

# Obtém o diretório atual onde o script principal está localizado
diretorio_atual = os.path.dirname(os.path.abspath(__file__))

# Lista os arquivos no diretório atual
arquivos = os.listdir(diretorio_atual)

# Filtra apenas os arquivos Python (.py)
scripts_python = [
    'g7.1--g7.2--t7.1.py',
    'g1.5--g1.6--g1.7--g1.8--t1.1.py',
    'g4.2.py',
    'g5.4.py',
    'g11.2.py',
    'g17.1.py',
    'g17.2.py',
    'g18.4.py',
    'g18.7.py',
    'g18.8.py',
    'g19.2--g19.4--g19.5--g19.6.py',
    'g20.1--g20.2.py',
    'g20.3--g20.4.py',
    'g20.11--g20.12.py',
    't17.1.py',
    't17.2.py',
    'g20.3--g20.4.py',
    'g19.1--g19.3--g19.7--g19.8--g19.10--g19.11--g19.12.py'
]

# Executa cada script Python encontrado
for script in scripts_python:
    print(f"Executando o script: {script}")
    caminho_script = os.path.join(diretorio_atual, script)
    
    # Usa subprocess para executar o script
    subprocess.run([sys.executable, caminho_script])
    sleep(3)
