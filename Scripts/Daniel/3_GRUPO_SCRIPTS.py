import datetime
import subprocess
import os
import sys
import platform
from multiprocessing import Pool, cpu_count
from functools import partial


# Detecta se está rodando no GitHub Actions ou local
def detect_environment():
    # GitHub Actions define variáveis de ambiente específicas
    if os.environ.get('GITHUB_ACTIONS', 'false').lower() == 'true':
        return 'github'
    # Pode adicionar outras detecções se necessário
    return 'local'

# Ajusta o número de workers e recursos baseado no ambiente
def get_optimal_workers():
    env = detect_environment()
    if env == 'github':
        # GitHub: 4 cores, 16GB RAM, 14GB SSD
        return 3  # Deixa 1 core livre
    else:
        # Local: i5-14600K (14 cores/20 threads), 16GB RAM, 400GB SSD
        total_cores = cpu_count()
        # Usa 75% dos cores, mas nunca mais que 14 (físicos)
        return min(14, max(1, int(total_cores * 0.75)))


# Obtém o mês atual
current_month = datetime.datetime.now().month

# Obtém o diretório atual onde o script principal está localizado
script_path = os.path.dirname(os.path.abspath(__file__))

scripts_to_run = [
    'g20.3--g20.4.py', 'g20.5--g20.6--g20.7--g20.8.py', 'g3.1--g3.2--g3.3--g3.4--g3.5--g3.6--g3.7--g3.8--g3.9--g3.10.py',
    'g4.1--g4.2--t4.1.py', 'g5.1--g5.2--g5.3--g5.4.py', 'g6.1--g6.2--g6.3--g6.4.py', 'g7.1--g7.2--t7.1.py', 'g8.1--g8.2--g8.3--g8.4.py',
    'g9.1--g9.2--t9.1.py', 't13.2.py', 't18.1.py', 't18.2.py'
]


def run_script(script_info):
    """Executa um script e retorna o resultado"""
    index, total, script, path = script_info
    print(f"[{index}/{total}] Executando o script: {script}", flush=True)
    result = subprocess.run(
        [sys.executable, os.path.join(path, script)],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        print(f"Script {script} finalizou com erro (código: {result.returncode})", flush=True)
        return (script, False, result.returncode)
    else:
        print(f"Script {script} concluído com sucesso", flush=True)
        return (script, True, 0)


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    print(f"{len(scripts_to_run)} scripts to run.", flush=True)
    env = detect_environment()
    num_workers = get_optimal_workers()
    print(f"Ambiente detectado: {env}", flush=True)
    print(f"Usando {num_workers} workers paralelos", flush=True)

    # Prepara os dados para cada script
    script_data = [
        (i, len(scripts_to_run), script, script_path)
        for i, script in enumerate(scripts_to_run, 1)
    ]

    # Executa os scripts em paralelo
    with Pool(processes=num_workers) as pool:
        results = pool.map(run_script, script_data)

    # Resume os resultados
    success_count = sum(1 for _, success, _ in results if success)
    failed_count = len(results) - success_count

    end_time = datetime.datetime.now()
    duration = (end_time - start_time).total_seconds()

    print(f"\n{'='*60}")
    print(f"Execução concluída em {duration:.2f} segundos")
    print(f"Sucesso: {success_count}/{len(scripts_to_run)}")
    print(f"Falhas: {failed_count}/{len(scripts_to_run)}")
    print(f"{'='*60}\n", flush=True)

    with open(os.path.abspath(os.path.join('Doc', 'relatorios_de_erros', 'update_log.txt')), 'w', encoding='utf-8') as f:
        f.write(
            f'''Executado(s) o(s) script(s) do Grupo 3 em paralelo
Data da execução: {datetime.datetime.today().strftime('%A, %d de %B de %Y - %H:%M:%S')}
Duração: {duration:.2f} segundos
Scripts executados: {len(scripts_to_run)}
Sucessos: {success_count}
Falhas: {failed_count}
'''
        )

    # Libera recursos (boa prática, mas Pool já faz isso)
    # Se necessário, pode adicionar limpeza extra aqui
    import gc
    gc.collect()
