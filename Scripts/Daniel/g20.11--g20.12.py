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
    regions = [('1', 'all'), ('2', '2'), ('3', '28')]
    dfs = []
    for reg in regions:
        data = sidrapy.get_table(table_code='7435', territorial_level=reg[0], ibge_territorial_code=reg[1],
                                 variable='10681', period='all', header='n')
        data = data[['D1N', 'D2N', 'V']]
        dfs.append(data)
        c.delay_requests(1)

    df_concat = pd.concat(dfs, ignore_index=True)
    df_concat.columns = ['Região', 'Ano', 'Índice de Gini']
    c.convert_type(df_concat, 'Ano', 'int')
    c.convert_type(df_concat, 'Índice de Gini', 'float')

    c.to_excel(df_concat, sheets_path, 'g20.12.xlsx')

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
