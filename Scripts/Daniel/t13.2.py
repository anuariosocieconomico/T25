"""
Esse script foi incluído ao script do grupo 13. Este aqui está comentado para evitar repetição de execução e para evitar confusão com o script do grupo 13.
"""

# import functions as c
# import os
# import pandas as pd
# import json
# import sidrapy
# import traceback
# import tempfile
# import shutil


# # obtém o caminho desse arquivo de comandos para adicionar os diretórios que armazenará as bases de dados e planilhas
# dbs_path = tempfile.mkdtemp()
# sheets_path = c.sheets_dir
# errors_path = c.errors_dir

# # inicializa o dicionário para armazenar informações sobre possíveis erros durante a execução
# errors = {}

# # ************************
# # DOWNLOAD DA BASE DE DADOS E PLANILHA
# # ************************

# try:
#     data = sidrapy.get_table(
#         table_code='5434',
#         territorial_level='3', ibge_territorial_code='28',
#         variable='4090,4108',
#         classifications={'888': '47947,47948,47949,47950,56622,56623,56624,60032'},
#         period="all"
#     )

#     # remoção da linha 0, dados para serem usados como rótulos das colunas
#     # não foram usados porque variam de acordo com a tabela
#     # seleção das colunas de interesse
#     data.drop(0, axis='index', inplace=True)
#     data = data[['MN', 'D1N', 'D2N', 'D3N', 'D4N', 'V']]

#     # separação de valores; valores inteiros e percentuais estão armazenados na mesma coluna
#     data_ab = data.loc[data['MN'] == 'Mil pessoas']
#     data_per = data.loc[data['MN'] == '%']
#     data = data_ab.iloc[:, 1:]
#     data['Percentual'] = data_per.loc[:, 'V'].to_list()

#     # renomeação das colunas
#     # filtragem de dados referentes ao 4º trimestre de cada ano
#     data.columns = ['Região', 'Trimestre', 'Classe', 'Variável', 'Pessoas', 'Percentual']
#     data = data.loc[data['Trimestre'].str.startswith('4º trimestre')].copy()
#     data['Trimestre'] = data['Trimestre'].apply(lambda x: '01/10/' + x[-4:])
#     data = data[['Região', 'Variável', 'Trimestre', 'Pessoas', 'Percentual']]

#     # classificação dos dados
#     data['Pessoas'] = data['Pessoas'].astype(int)
#     data['Percentual'] = data['Percentual'].astype(float)

#     # conversão em arquivo csv
#     c.to_excel(data, sheets_path, 't13.2.xlsx')

# except Exception as e:
#     errors['Tabela 13.2'] = traceback.format_exc()

# # geração do arquivo de erro caso ocorra algum
# # se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# # se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
# if errors:
#     with open(os.path.join(errors_path, 'script--t13.2.txt'), 'w', encoding='utf-8') as f:
#         f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# # remove os arquivos baixados
# shutil.rmtree(dbs_path)
