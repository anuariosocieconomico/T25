import functions as c
import os
import pandas as pd
import json
import sidrapy
import traceback
import tempfile
import shutil


# obtém o caminho desse arquivo de comandos para adicionar os diretórios que armazenará as bases de dados e planilhas
dbs_path = tempfile.mkdtemp()
sheets_path = c.sheets_dir
errors_path = c.errors_dir

# inicializa o dicionário para armazenar informações sobre possíveis erros durante a execução
errors = {}

# ************************
# DOWNLOAD DA BASE DE DADOS E PLANILHA
# ************************

try:
    tables = ['356', '7126']
    t_level_code = [('1', 'all'), ('2', '2'), ('3', '28')]
    var_code = ['1887', '3593']
    class_ = [{'1': '6795', '2': '6794', '58': '2795'}, {'2': '6794', '58': '2795'}]

    datasets = []
    for tb, var, cs in zip(tables, var_code, class_):
        dfs = []
        for level in t_level_code:
            data = sidrapy.get_table(table_code=tb, territorial_level=level[0], ibge_territorial_code=level[1],
                                     variable=var, classifications=cs, period='all')
            data.columns = data.iloc[0]
            data.drop(0, axis='index', inplace=True)
            col = data.columns[6]
            data.rename(columns={col: 'Região'}, inplace=True)
            dfs.append(data)
        df_concat = pd.concat(dfs, ignore_index=True)
        datasets.append(df_concat)

    for i, df in enumerate(datasets):
        datasets[i] = datasets[i].iloc[:, [6, 8, 4]].copy()

        datasets[i]['Região'] = datasets[i]['Região'].astype('str')
        datasets[i]['Ano'] = pd.to_datetime(datasets[i]['Ano'], format='%Y')
        datasets[i]['Ano'] = datasets[i]['Ano'].dt.strftime('%d/%m/%Y')
        datasets[i]['Valor'] = datasets[i]['Valor'].astype('float64')

    df = pd.concat(datasets, ignore_index=True)
    df.sort_values(by=['Região', 'Ano'], inplace=True)

    c.to_excel(df, sheets_path, 'g17.2.xlsx')

except Exception as e:
    errors['Gráfico 17.2'] = traceback.format_exc()


# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g17.2.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
