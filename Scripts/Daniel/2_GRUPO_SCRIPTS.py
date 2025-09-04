import datetime
import subprocess
import os
import sys

# Obtém o mês atual
current_month = datetime.datetime.now().month

# Obtém o diretório atual onde o script principal está localizado
script_path = os.path.dirname(os.path.abspath(__file__))

# Scripts do grupo 2
scripts_to_run = [
    'g18.4.py',
    'g18.5.py',
    'g18.6.py',
    'g18.7.py',
    'g18.8.py',
    'g19.1--g19.3--g19.7--g19.8--g19.10--g19.11--g19.12.py',
    'g19.14.py',
    'g19.2--g19.4--g19.5--g19.6.py',
    'g19.9.py',
    'g2.1--g2.2--g2.3--g2.4--g2.5--g2.6.py',
    'g20.1--g20.2.py',
    'g20.11--g20.12.py'
]

# Executa os scripts selecionados
for script in scripts_to_run:
    subprocess.run([sys.executable, os.path.join(script_path, script)])

with open(os.path.abspath(os.path.join('Doc', 'relatorios_de_erros', 'update_log.txt')), 'w', encoding='utf-8') as f:
    f.write(
        f'''Executado(s) o(s) scipt(s) do Grupo 2 ...
Data da execução: {datetime.datetime.today().strftime('%A, %d de %B de %Y - %H:%M:%S')}.'''
    )
