import functions as c
import os
import pandas as pd
import json
import io
import sidrapy
from bs4 import BeautifulSoup
import traceback
import tempfile
import shutil
from datetime import datetime
from time import sleep


# obtém o caminho desse arquivo de comandos para adicionar os diretórios que armazenará as bases de dados e planilhas
dbs_path = tempfile.mkdtemp()
sheets_path = c.sheets_dir
errors_path = c.errors_dir

# inicializa o dicionário para armazenar informações sobre possíveis erros durante a execução
errors = {}


# ************************
# DOWNLOAD DA BASE DE DADOS E PLANILHA
# ************************

# Gráfico 16.1
try:
    # looping de requisições para cada tabela da figura
    dfs = []
    for reg in [('1', 'all'), ('2', '2'), ('3', '28')]:
        data = sidrapy.get_table(
            table_code='5442',
            territorial_level=reg[0],ibge_territorial_code=reg[1],
            variable='5934',
            classifications={'888': '47946'},
            period="all"
        )

        # remoção da linha 0, dados para serem usados como rótulos das colunas
        data.drop(0, axis='index', inplace=True)

        dfs.append(data)

    data = pd.concat(dfs, ignore_index=True)

    # seleção das colunas de interesse
    data = data[['D1N', 'D3N', 'D2N', 'V']].copy()
    data.columns = ['Região', 'Variável', 'Ano', 'Valor']
    data['Trimestre'] = data['Ano'].apply(lambda x: x[0]).astype(int)
    data['Ano'] = data['Ano'].apply(lambda x: x.split(' ')[-1]).astype(int)
    data['Valor'] = data['Valor'].replace('...', 0).astype(float)  # valores nulos são definidos por '...'

    # conversão em arquivo csv
    c.to_csv(data, dbs_path, 'base_rendimento.csv')

except Exception as e:
    errors['Base de rendimentos'] = traceback.format_exc()


# DOWNLOAD DA BASE DE DADOS IPCA ---------------------------------------------------------------------------------------
url = 'http://www.ipeadata.gov.br/ExibeSerie.aspx?serid=1410807112&module=M'
try:
    response = c.open_url(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    tbs = pd.read_html(io.StringIO(str(soup)), thousands='.', decimal=',')  # leitura de todas as tabelas da html
    df = [tb for tb in tbs if tb.shape[0] > 30][0]  # seleciona a tabela que houver + 30 linhas
    df = df.iloc[1:].copy()  # remoção de ruído na linha 0
    df.columns = ['Ano', 'IPCA']  # renomeação das colunas

    # cálculo dos índices
    df.sort_values(by='Ano', ascending=False, inplace=True)  # ordenação descendente dos anos
    df['Index'] = 0.0  # inicialização da coluna de índice
    df['IPCA'] = df['IPCA'].astype(float) / 100  # conversão da porcentagem para fração
    df.iloc[0, -1] = 100  # atribuição do valor 100 ao primeiro ano da série

    for i in range(len(df)):
        if i == 0:
            continue
        else:
            df.iloc[i, -1] = df.iloc[i-1, -1] / (1 + df.iloc[i-1, -2])

    c.to_csv(df, dbs_path, 'tb-ipca.csv')
except:
    errors[url] = traceback.format_exc()


# ************************
# PLANILHA
# ************************

# g14.1
try:
    # leitura da base de dados
    data = c.open_file(dbs_path, 'base_rendimento.csv', 'csv')
    ipca = c.open_file(dbs_path, 'tb-ipca.csv', 'csv')

    # união das bases de dados
    df_merge = data.merge(ipca, how='left', on='Ano', validate='m:1')
    df_merge['Taxa'] = (df_merge['Valor'] / df_merge['Index']) * 100

except:
    errors['Gráfico 14.1'] = traceback.format_exc()

# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'g14.1--g14.2--t14.1--t14.2.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
