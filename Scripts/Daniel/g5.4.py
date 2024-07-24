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
    # looping de requisições para cada tabela da figura
    data = sidrapy.get_table(table_code='1761', territorial_level='3', ibge_territorial_code='28', variable='631,1243',
                             period="all")

    # remoção da linha 0, dados para serem usados como rótulos das colunas
    # não foram usados porque variam de acordo com a tabela
    # seleção das colunas de interesse
    data.drop(0, axis='index', inplace=True)
    data = data[['D1N', 'D2N', 'D3N', 'V']]

    # renomeação das colunas
    # filtragem de dados a partir do ano 2010
    data.columns = ['Região', 'Ano', 'Variável', 'Valor']
    data = data.loc[data['Ano'] >= '2010'].copy()

    # classificação dos dados
    data[['Região', 'Variável']] = data[['Região', 'Variável']].astype('str')
    data['Ano'] = pd.to_datetime(data['Ano'], format='%Y')
    data['Ano'] = data['Ano'].dt.strftime('%d/%m/%Y')
    data['Valor'] = data['Valor'].astype('int64')

    # adicionado após conversa
    data = data.pivot(index=['Região', 'Ano'], columns='Variável', values='Valor').reset_index()
    data.index.name = None

    # conversão em arquivo csv
    c.to_excel(data, sheets_path, 'g5.4.xlsx')

except Exception as e:
    errors['Gráfico 5.4'] = traceback.format_exc()

# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g5.4.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
