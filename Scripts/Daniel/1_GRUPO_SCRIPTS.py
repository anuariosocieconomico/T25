import datetime
import subprocess
import os
import sys

# Obtém o mês atual
current_month = datetime.datetime.now().month

# Obtém o diretório atual onde o script principal está localizado
script_path = os.path.dirname(os.path.abspath(__file__))

# Scripts do grupo 1
scripts_to_run = [
    'g1.5--g1.6--g1.7--g1.8--t1.1.py',
    'g10.1--g10.2--t10.1.py',
    # 'g11.1--ate--g11.8--t11.1--t11.2.py',
    # 'g12.1--g12.2--t12.1.py',
    # 'g13.1a--ate-t13.3.py',
    # 'g13.6.py',
    # 'g14.1--g14.2--t14.1--t14.2.py',
    # 'g15.1--g15.2--g15.3--g15.4.py',
    # 'g16.1--t16.1--g16.3--g16.4--t16.2.py',
    # 'g17.1--g17.2--g17.3--g17.4--t17.1--t17.2.py',
    'g18.1--g18.2.py',
    'g18.3.py'
]

# Executa os scripts selecionados
for script in scripts_to_run:
    subprocess.run([sys.executable, os.path.join(script_path, script)])

with open(os.path.abspath(os.path.join('Doc', 'relatorios_de_erros', 'update_log.txt')), 'w', encoding='utf-8') as f:
    f.write(
        f'''Executado(s) o(s) scipt(s) do Grupo 1 ...
Data da execução: {datetime.datetime.today().strftime('%A, %d de %B de %Y - %H:%M:%S')}.'''
    )
