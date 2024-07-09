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
    tables = ['1187', '7113']
    t_level_code = [('1', 'all'), ('2', '2'), ('3', '28')]
    var_code = ['2513', '10267']
    class_ = ['2', ('2', '58')]
    class_val = ['6794', ('6794', '2795')]

    datasets = []
    for tb, var, cs, cs_v in zip(tables, var_code, class_, class_val):
        if not isinstance(cs, tuple):
            dfs = []
            for level in t_level_code:
                data = sidrapy.get_table(table_code=tb, territorial_level=level[0], ibge_territorial_code=level[1],
                                         variable=var, classifications={cs: cs_v}, period='all')
                data.columns = data.iloc[0]
                data.drop(0, axis='index', inplace=True)
                col = data.columns[6]
                data.rename(columns={col: 'Região'}, inplace=True)
                dfs.append(data)
            df_concat = pd.concat(dfs, ignore_index=True)
            datasets.append(df_concat)
        else:
            dfs = []
            for level in t_level_code:
                data = sidrapy.get_table(table_code=tb, territorial_level=level[0], ibge_territorial_code=level[1],
                                         variable=var, classifications={cs[0]: cs_v[0], cs[1]: cs_v[1]}, period='all')
                data.columns = data.iloc[0]
                data.drop(0, axis='index', inplace=True)
                col = data.columns[6]
                data.rename(columns={col: 'Região'}, inplace=True)
                dfs.append(data)
            df_concat = pd.concat(dfs, ignore_index=True)
            datasets.append(df_concat)

    dfs = []
    for i, temp in enumerate(datasets):
        df = temp.iloc[:, [6, 8, 4]].copy()

        df[df.columns[0]] = df[df.columns[0]].astype('str')
        df[df.columns[1]] = pd.to_datetime(df[df.columns[1]], format='%Y')
        df[df.columns[1]] = df[df.columns[1]].dt.strftime('%d/%m/%Y')
        df[df.columns[2]] = df[df.columns[2]].astype('float64')

        if i == 0:
            df['Taxa de analfabetismo'] = 100 - df['Valor']
            df.drop('Valor', axis='columns', inplace=True)
        else:
            df.rename(columns={'Valor': 'Taxa de analfabetismo'}, inplace=True)
        dfs.append(df)

    df_concat = pd.concat(dfs, ignore_index=True)
    df_concat.sort_values(by=['Região', 'Ano'], inplace=True)

    c.to_excel(df_concat, sheets_path, 'g17.1.xlsx')

except Exception as e:
    errors['Gráfico 17.1'] = traceback.format_exc()


# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g17.1.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
