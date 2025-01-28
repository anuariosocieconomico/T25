import datetime
import subprocess
import os
import sys

# Obtém o mês atual
current_month = datetime.datetime.now().month

# Obtém o diretório atual onde o script principal está localizado
script_path = os.path.dirname(os.path.abspath(__file__))

# Define os scripts a serem executados com base no mês
if current_month == 1:
    scripts_to_run = ['run_annual_update.py', 'run_monthly_update.py']
elif 2 <= current_month <= 5:
    scripts_to_run = ['run_monthly_update.py']
elif current_month == 6:
    scripts_to_run = ['run_annual_update.py', 'run_monthly_update.py']
elif 7 <= current_month <= 11:
    scripts_to_run = ['run_monthly_update.py']
else:
    scripts_to_run = ['run_annual_update.py', 'run_monthly_update.py']

# Executa os scripts selecionados
for script in scripts_to_run:
    subprocess.run([sys.executable, os.path.join(script_path, script)])

with open(os.path.abspath(os.path.join('Doc', 'relatorios_de_erros', 'update_log.txt')), 'w', encoding='utf-8') as f:
    f.write(
        f'''Executado(s) o(s) scipt(s): {scripts_to_run} ...
Data da execução: {datetime.datetime.today().strftime('%A, %d de %B de %Y - %H:%M:%S')}.'''
    )
