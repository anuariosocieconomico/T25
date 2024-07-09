import functions as c
import os
import pandas as pd
import json
import requests
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

url = 'https://servicodados.ibge.gov.br/api/v1/downloads/estatisticas/?caminho=Indicadores_Sociais' \
      '/Sintese_de_Indicadores_Sociais'

try:
    response = requests.get(url, headers=c.headers)

    url_to_last_pub = response.json()[-2]['path'].split('/')[-1] + '/xls'
    response = requests.get(url + '/' + url_to_last_pub)

    df_pubs = pd.DataFrame(response.json())
    link = df_pubs.loc[
        (df_pubs['name'].str.contains('Padrao_de_vida')) & (df_pubs['name'].str.endswith('xls.zip')), 'url'
    ].values[0]
    file = requests.get(link)

    data = c.open_file(file_path=file.content, ext='zip', excel_name='Tabela 2.13', skiprows=7)
    tables = [tb for tb in data.keys() if '(CV)' not in tb]

    dfs = []
    for tb in tables:
        df = data[tb]
        df.columns = ['Região', 'Índice de Gini']
        i = df.loc[df['Região'] == 'Brasília'].index.values[0]
        df_filtered = df.iloc[:i + 1].copy()
        df_filtered['Ano'] = str(tb)
        dfs.append(df_filtered)

    df_concat = pd.concat(dfs, ignore_index=True)

    c.to_csv(df_concat, dbs_path, 'ibge (ÍNDICE GINI).csv')

except Exception as e:
    errors[url] = traceback.format_exc()


# ************************
# PLANILHA
# ************************

'''
COMANDO IGNORADO PORQUE A PLANILHA G20.11 FOI AUTOMATIZADA POR RODRIGO, ELE A ESTRUTUROU DE FORMA DIFERENTE.
SE ADICIONAR ESSA AO GITHUB RETORNARÁ ERRO NA FIGURA DO DASHBOARD!
'''

# # g20.11
# try:
#     data = c.open_file(dbs_path, 'ibge (ÍNDICE GINI).csv', 'csv')
#
#     df = data.loc[
#         (data['Ano'] == data['Ano'].min()) | (data['Ano'] == data['Ano'].max())
#         ].drop_duplicates(subset=['Região', 'Ano']).pivot(
#         index='Região', columns='Ano', values='Índice de Gini'
#     ).reset_index().copy()
#
#     br_states = ['Rondônia', 'Acre', 'Amazonas', 'Roraima', 'Pará', 'Amapá', 'Tocantins', 'Maranhão', 'Piauí', 'Ceará',
#                  'Rio Grande do Norte', 'Paraíba', 'Pernambuco', 'Alagoas', 'Sergipe', 'Bahia', 'Minas Gerais',
#                  'Espírito Santo', 'Rio de Janeiro', 'São Paulo', 'Paraná', 'Santa Catarina', 'Rio Grande do Sul',
#                  'Mato Grosso do Sul', 'Mato Grosso', 'Goiás', 'Distrito Federal']
#
#     df_states = df.loc[
#         df['Região'].isin(br_states + ['Brasil', 'Nordeste'])
#     ].sort_values(by=df.columns[-1], ascending=False).reset_index(drop=True).copy()
#
#     df_states['Variação'] = df_states[df_states.columns[-1]] - df_states[df_states.columns[-2]]
#     df_states.columns = [col if isinstance(col, str) else f'Gini/{str(col)}' for col in df_states.columns]
#
#     c.to_excel(df_states, sheets_path, 'g20.11.xlsx')
#
# except Exception as e:
#     errors['Gráfico 20.11'] = traceback.format_exc()

# g20.12
try:
    data = c.open_file(dbs_path, 'ibge (ÍNDICE GINI).csv', 'csv')

    br_states = ['Rondônia', 'Acre', 'Amazonas', 'Roraima', 'Pará', 'Amapá', 'Tocantins', 'Maranhão', 'Piauí', 'Ceará',
                 'Rio Grande do Norte', 'Paraíba', 'Pernambuco', 'Alagoas', 'Sergipe', 'Bahia', 'Minas Gerais',
                 'Espírito Santo', 'Rio de Janeiro', 'São Paulo', 'Paraná', 'Santa Catarina', 'Rio Grande do Sul',
                 'Mato Grosso do Sul', 'Mato Grosso', 'Goiás', 'Distrito Federal']

    df_unique = data.drop_duplicates(subset=['Região', 'Ano']).copy()

    df_states = df_unique.loc[df_unique['Região'].isin(br_states + ['Brasil', 'Nordeste'])].copy()
    df_states = df_states[['Região', 'Ano', 'Índice de Gini']].copy()
    df_states.sort_values(by=['Região', 'Ano'], inplace=True)

    c.convert_type(df_states, 'Ano', 'datetime')

    c.to_excel(df_states, sheets_path, 'g20.12.xlsx')

except Exception as e:
    errors['Gráfico 20.12'] = traceback.format_exc()

# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g20.11--g20.12.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
