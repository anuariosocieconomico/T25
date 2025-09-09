import datetime
import subprocess
import os
import sys

# Obtém o mês atual
current_month = datetime.datetime.now().month

# Obtém o diretório atual onde o script principal está localizado
script_path = os.path.dirname(os.path.abspath(__file__))

# Scripts do grupo 1
# scripts_to_run = [
#     'g1.5--g1.6--g1.7--g1.8--t1.1.py',
#     'g10.1--g10.2--t10.1.py',
#     # 'g11.1--ate--g11.8--t11.1--t11.2.py',
#     # 'g12.1--g12.2--t12.1.py',
#     # 'g13.1a--ate-t13.3.py',
#     # 'g13.6.py',
#     # 'g14.1--g14.2--t14.1--t14.2.py',
#     # 'g15.1--g15.2--g15.3--g15.4.py',
#     # 'g16.1--t16.1--g16.3--g16.4--t16.2.py',
#     # 'g17.1--g17.2--g17.3--g17.4--t17.1--t17.2.py',
#     'g18.1--g18.2.py',
#     'g18.3.py'
# ]

# # TODOS OS SCRIPTS PARA TESTAR ADAPTABILDIADE NO RUNNER DO GITHUB
# scripts_to_run = [
#     'g1.5--g1.6--g1.7--g1.8--t1.1.py', 'g10.1--g10.2--t10.1.py', 'g11.1--ate--g11.8--t11.1--t11.2.py', 'g12.1--g12.2--t12.1.py',
#     'g13.1a--ate-t13.3.py', 'g13.6.py', 'g14.1--g14.2--t14.1--t14.2.py', 'g15.1--g15.2--g15.3--g15.4.py',
#     'g16.1--t16.1--g16.3--g16.4--t16.2.py', 'g17.1--g17.2--g17.3--g17.4--t17.1--t17.2.py', 'g18.1--g18.2.py', 'g18.3.py',
#     'g18.4.py', 'g18.5.py', 'g18.6.py', 'g18.7.py', 'g18.8.py', 'g19.1--g19.3--g19.7--g19.8--g19.10--g19.11--g19.12.py', 'g19.14.py',
#     'g19.2--g19.4--g19.5--g19.6.py', 'g19.9.py', 'g2.1--g2.2--g2.3--g2.4--g2.5--g2.6.py', 'g20.1--g20.2.py', 'g20.11--g20.12.py',
#     'g20.3--g20.4.py', 'g20.5--g20.6--g20.7--g20.8.py', 'g3.1--g3.2--g3.3--g3.4--g3.5--g3.6--g3.7--g3.8--g3.9--g3.10.py',
#     'g4.1--g4.2--t4.1.py', 'g5.1--g5.2--g5.3--g5.4.py', 'g6.1--g6.2--g6.3--g6.4.py', 'g7.1--g7.2--t7.1.py', 'g8.1--g8.2--g8.3--g8.4.py',
#     'g9.1--g9.2--t9.1.py', 't13.2.py', 't18.1.py', 't18.2.py'
#     ]

# para testes
scripts_to_run = [
    'g3.1--g3.2--g3.3--g3.4--g3.5--g3.6--g3.7--g3.8--g3.9--g3.10.py',
    # 'g4.1--g4.2--t4.1.py',
    'g5.1--g5.2--g5.3--g5.4.py',
    'g6.1--g6.2--g6.3--g6.4.py',
    # 'g10.1--g10.2--t10.1.py',
    'g11.1--ate--g11.8--t11.1--t11.2.py',
    'g15.1--g15.2--g15.3--g15.4.py',
    'g16.1--t16.1--g16.3--g16.4--t16.2.py',
    'g17.1--g17.2--g17.3--g17.4--t17.1--t17.2.py',
    # 'g20.5--g20.6--g20.7--g20.8.py'
    ]

# Executa os scripts selecionados
for script in scripts_to_run:
    subprocess.run([sys.executable, os.path.join(script_path, script)])

with open(os.path.abspath(os.path.join('Doc', 'relatorios_de_erros', 'update_log.txt')), 'w', encoding='utf-8') as f:
    f.write(
        f'''Executado(s) o(s) scipt(s) do Grupo 1 ...
Data da execução: {datetime.datetime.today().strftime('%A, %d de %B de %Y - %H:%M:%S')}.'''
    )
