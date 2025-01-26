import os
import subprocess
from time import sleep

# Obtém o diretório atual onde o script principal está localizado
diretorio_atual = os.path.dirname(os.path.abspath(__file__))

# Lista os arquivos no diretório atual
arquivos = os.listdir(diretorio_atual)

# Filtra apenas os arquivos Python (.py)
# scripts_python = [
#     'g19.14.py',
#     'g8.1--g8.2.py',
#     'g18.5.py',
#     'g18.6.py',
#     't18.1.py',
#     't18.2.py',
#     'g11.11.py',
#     't13.2.py',
#     'g13.6.py'
# ]

scripts_python = [
    'g8.1--g8.2.py',
    'g18.5.py'
]

# Executa cada script Python encontrado
for script in scripts_python:
    print(f"Executando o script: {script}")
    caminho_script = os.path.join(diretorio_atual, script)
    
    # Usa subprocess para executar o script
    subprocess.run(['python', caminho_script])
    sleep(3)
