import datetime
import subprocess
import os
import sys

# Obtém o mês atual
current_month = datetime.datetime.now().month

# Obtém o diretório atual onde o script principal está localizado
script_path = os.path.dirname(os.path.abspath(__file__))

# Scripts do grupo 3
scripts_to_run = [
    'g20.3--g20.4.py',
    'g20.5--g20.6--g20.7--g20.8.py',
    'g3.1--g3.2--g3.3--g3.4--g3.5--g3.6--g3.7--g3.8--g3.9--g3.10.py',
    'g4.1--g4.2--t4.1.py',
    'g5.1--g5.2--g5.3--g5.4.py',
    'g6.1--g6.2--g6.3--g6.4.py',
    'g7.1--g7.2--t7.1.py',
    'g8.1--g8.2--g8.3--g8.4.py',
    'g9.1--g9.2--t9.1.py',
    't13.2.py',
    't18.1.py',
    't18.2.py'
]

# Executa os scripts selecionados
for script in scripts_to_run:
    subprocess.run([sys.executable, os.path.join(script_path, script)])

with open(os.path.abspath(os.path.join('Doc', 'relatorios_de_erros', 'update_log.txt')), 'w', encoding='utf-8') as f:
    f.write(
        f'''Executado(s) o(s) scipt(s) do Grupo 3 ...
Data da execução: {datetime.datetime.today().strftime('%A, %d de %B de %Y - %H:%M:%S')}.'''
    )
