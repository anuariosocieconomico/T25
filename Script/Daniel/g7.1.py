import functions as c
import os
import pandas as pd
import numpy as np
import json
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
# DOWNLOAD DA BASE DE DADOS
# ************************

# url
url = 'https://www.epe.gov.br/pt/publicacoes-dados-abertos/publicacoes/anuario-estatistico-de-energia-eletrica'

# tenta baixar o arquivo conforme os comandos anteriores; caso haja alguma atualização no site, registra-se o erro
try:
    # caminho xpath de acesso ao arquivo para download
    xpath = '/html/body/form/div[3]/div[2]/section/div[2]/div/span/div[2]/div[1]/div[5]/div/div/div/div[' \
            '2]/div/table/tbody/tr/td[2]/a/@href'

    # request ao site e extração dos elementos da html
    html = c.get_html(url)
    html_urls = html.xpath(xpath).getall()
    url_to_get = [item for item in html_urls if item.endswith('.xlsx')][0]
    url_to_get = 'https://www.epe.gov.br' + url_to_get
    file = c.open_url(url_to_get)

    # salva os dados em arquivo local
    c.to_file(dbs_path, 'epe-anuario-energia.xlsx', file.content)

# registro do erro em caso de atualizações
except Exception as e:
    errors[url] = traceback.format_exc()


# ************************
# PLANILHA
# ************************

try:
    # organização do arquivo
    # seleção das colunas e das linhas de interesse
    df = c.open_file(dbs_path, 'epe-anuario-energia.xlsx',
                     'xls', sheet_name='Tabela 3.63', skiprows=8)
    df = df.iloc[:, 1:12]
    df = df.loc[df[' '].isin(['Brasil', 'Nordeste', 'Sergipe'])]

    # reordenação da variável ano para o eixo y
    # renomeação do rótulo da coluna de ' ' para 'Região'
    # ordenação alfabética da coluna 'Região' e cronológica da coluna 'Ano'
    df_melted = df.melt(id_vars=' ', value_vars=list(df.columns[1:]), var_name='Ano',
                        value_name='Valor')
    df_melted.rename(columns={' ': 'Região'}, inplace=True)
    df_melted.sort_values(by=['Região', 'Ano'], ascending=[True, True], inplace=True)

    # classificação dos valores em cada coluna
    df_melted[df_melted.columns[0]] = df_melted[df_melted.columns[0]].astype('str')
    df_melted[df_melted.columns[1]] = pd.to_datetime(df_melted[df_melted.columns[1]], format='%Y')
    df_melted[df_melted.columns[1]] = df_melted[df_melted.columns[1]].dt.strftime('%d/%m/%Y')
    df_melted[df_melted.columns[2]] = df_melted[df_melted.columns[2]].astype('float64')

    # tratamento do título da figura para nomeação do arquivo csv
    c.to_excel(df_melted, sheets_path, 'g7.1.xlsx')

except Exception as e:
    errors['Gráfico 7.1'] = traceback.format_exc()

# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g7.1.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
