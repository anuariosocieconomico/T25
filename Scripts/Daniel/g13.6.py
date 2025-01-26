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
    regions = [('1', 'all'), ('2', '2'), ('3', '28')]
    dfs = []
    for reg in regions:
        data = sidrapy.get_table(
            table_code='6402', territorial_level=reg[0],
            ibge_territorial_code=reg[1],
            variable='4099',
            classifications={'86': '95251'},
            period="all"
        )

        # remoção da linha 0, dados para serem usados como rótulos das colunas
        # não foram usados porque variam de acordo com a tabela
        # seleção das colunas de interesse
        data.drop(0, axis='index', inplace=True)
        data = data[['D1N', 'D2N', 'D3N', 'D4N', 'V']]
        dfs.append(data)

        # acrescenta delay às requests
        c.delay_requests(2)

    # união dos dfs
    # renomeação das colunas
    # filtragem de dados referentes ao 4º trimestre de cada ano
    # seleção dos dígitos referentes ao ano
    df_concat = pd.concat(dfs, ignore_index=True)
    df_concat.columns = ['Região', 'Trimestre', 'Variável', 'Classe', 'Valor']
    df_concat = df_concat.loc[df_concat['Trimestre'].str.startswith('4º trimestre')].copy()
    df_concat['Trimestre'] = df_concat['Trimestre'].apply(lambda x: '01/10/' + x[-4:])

    # classificação dos dados
    df_concat['Valor'] = df_concat['Valor'].replace('...', '0.0')
    df_concat['Valor'] = df_concat['Valor'].astype('float64')

    df_concat = df_concat[['Região', 'Variável', 'Trimestre', 'Valor']]

    # conversão em arquivo csv
    c.to_excel(df_concat, sheets_path, 'g13.6.xlsx')

except Exception as e:
    errors['Gráfico 13.6'] = traceback.format_exc()

# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g13.6.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)