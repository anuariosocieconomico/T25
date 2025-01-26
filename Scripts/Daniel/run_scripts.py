import datetime
import subprocess

# Obtém o mês atual
current_month = datetime.datetime.now().month

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
    subprocess.run(['python', script])
