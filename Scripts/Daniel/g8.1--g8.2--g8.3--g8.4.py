import functions as c
import os
import pandas as pd
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
url = 'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/producao-de-petroleo-e-gas-natural-por-estado' \
    '-e-localizacao'

try:
    # downloading do arquivo
    xpath = '/html/body/div[2]/div[1]/main/div[2]/div/div[4]/div/ul[1]/li[2]/a/@href'
    html = c.get_html(url)
    url_to_get = html.xpath(xpath).get()
    file = c.open_url(url_to_get)

    c.to_file(dbs_path, 'anp_producao_petroleo.csv', file.content)
except Exception as e:
    errors[url + '(PRODUÇÃO DE PETRÓLEO)'] = traceback.format_exc()

try:
    # downloading do arquivo
    xpath = '/html/body/div[2]/div[1]/main/div[2]/div/div[4]/div/ul[3]/li[2]/a/@href'
    html = c.get_html(url)
    url_to_get = html.xpath(xpath).get()
    file = c.open_url(url_to_get)

    c.to_file(dbs_path, 'anp_producao_gas.csv', file.content)

    xpath = '/html/body/div[2]/div[1]/main/div[2]/div/div[4]/div/ul[2]/li[2]/a/@href'
    url_to_get = html.xpath(xpath).get()
    file = c.open_url(url_to_get)

    c.to_file(dbs_path, 'anp_producao_lgn.csv', file.content)
except Exception as e:
    errors[url + '(PRODUÇÃO DE GÁS)'] = traceback.format_exc()


# ************************
# PLANILHAS
# ************************

# g8.1
try:
    # organização do arquivo
    # dados sobre sergipe
    # remoção de variáveis não utilizáveis
    # adição da identificação da região
    df = c.open_file(dbs_path, 'anp_producao_petroleo.csv', 'csv', sep=';')
    df_se = df.loc[df['UNIDADE DA FEDERAÇÃO'] == 'SERGIPE']
    df_se = df_se.drop(['GRANDE REGIÃO', 'UNIDADE DA FEDERAÇÃO', 'PRODUTO'], axis='columns')
    df_se['REGIÃO'] = 'SERGIPE'

    month_mapping = {'jan': 1, 'fev': 2, 'mar': 3, 'abr': 4, 'mai': 5, 'jun': 6,
                    'jul': 7, 'ago': 8, 'set': 9, 'out': 10, 'nov': 11, 'dez': 12}
    df_concat = df_se
    df_concat['month'] = df_concat['MÊS'].str.lower().map(month_mapping)
    df_concat['date'] = df_concat['ANO'].astype('str') + '-' + df_concat['month'].astype('str')
    df_concat['ANO'] = df_concat['date']
    df_concat.drop(['month', 'date', 'MÊS'], axis='columns', inplace=True)

    # classificação dos dados
    df_concat[df_concat.columns[0]] = pd.to_datetime(df_concat[df_concat.columns[0]], format='%Y-%m')
    df_concat[df_concat.columns[0]] = df_concat[df_concat.columns[0]].dt.strftime('%d/%m/%Y')
    df_concat[df_concat.columns[1]] = df_concat[df_concat.columns[1]].astype('str')
    df_concat[df_concat.columns[2]] = df_concat[df_concat.columns[2]].astype('float64')
    df_concat[df_concat.columns[-1]] = df_concat[df_concat.columns[-1]].astype('str')

    c.to_excel(df_concat, sheets_path, 'g8.1.xlsx')
except Exception as e:
    errors['Gráfico 8.1'] = traceback.format_exc()

# g8.2
try:
    # abertura arquivo 1
    # seleção das variáveis de interesse
    # adição da variável 'REGIÃO'
    df = c.open_file(dbs_path, 'anp_producao_gas.csv', 'csv', sep=';')
    df_se = df.loc[df['UNIDADE DA FEDERAÇÃO'] == 'SERGIPE']
    df_se = df_se.groupby(['ANO', 'MÊS', 'LOCALIZAÇÃO', 'PRODUTO'])['PRODUÇÃO'].sum().reset_index()
    df_se['REGIÃO'] = 'SERGIPE'
    df_gas = df_se

    # abertura arquivo 2
    # seleção das variáveis de interesse
    # adição da variável 'LOCALIZAÇÃO'
    # adição da variável 'REGIÃO'
    df = c.open_file(dbs_path, 'anp_producao_lgn.csv', 'csv', sep=';')
    df_se = df.loc[df['UNIDADE DA FEDERAÇÃO'] == 'SERGIPE']
    df_se = df_se.groupby(['ANO', 'MÊS', 'PRODUTO'])['PRODUÇÃO'].sum().reset_index()
    df_se['LOCALIZAÇÃO'] = 'NÃO SE APLICA'
    df_se['REGIÃO'] = 'SERGIPE'

    df_lgn = df_se
    df_lgn = df_lgn[['ANO', 'MÊS', 'LOCALIZAÇÃO', 'PRODUTO', 'PRODUÇÃO', 'REGIÃO']]

    # união das tabelas de ambos os arquivos
    # classificação dos dados
    df_concat = pd.concat([df_gas, df_lgn], ignore_index=True)

    # adicionado após comentários acima
    month_mapping = {'jan': 1, 'fev': 2, 'mar': 3, 'abr': 4, 'mai': 5, 'jun': 6,
                    'jul': 7, 'ago': 8, 'set': 9, 'out': 10, 'nov': 11, 'dez': 12}
    df_concat['month'] = df_concat['MÊS'].str.lower().map(month_mapping)
    df_concat['date'] = df_concat['ANO'].astype('str') + '-' + df_concat['month'].astype('str')
    df_concat['ANO'] = df_concat['date']
    df_concat.drop(['month', 'date', 'MÊS'], axis='columns', inplace=True)

    df_concat[df_concat.columns[0]] = pd.to_datetime(df_concat[df_concat.columns[0]], format='%Y-%m')
    df_concat[df_concat.columns[0]] = df_concat[df_concat.columns[0]].dt.strftime('%d/%m/%Y')
    df_concat[df_concat.columns[1:3]] = df_concat[df_concat.columns[1:3]].astype('str')
    df_concat[df_concat.columns[-2]] = df_concat[df_concat.columns[-2]].astype('float64')
    df_concat[df_concat.columns[-1]] = df_concat[df_concat.columns[-1]].astype('str')

    # tratamento do título da figura para nomeação do arquivo
    # conversão em arquivo csv
    c.to_excel(df_concat, sheets_path, 'g8.2.xlsx')

except Exception as e:
    errors['Gráfico 8.2'] = traceback.format_exc()


# g8.3
try:
    df_petroleo = c.open_file(dbs_path, 'anp_producao_petroleo.csv', 'csv', sep=';')
    df_gas = c.open_file(dbs_path, 'anp_producao_gas.csv', 'csv', sep=';')
    df_lgn = c.open_file(dbs_path, 'anp_producao_lgn.csv', 'csv', sep=';')
    # Insere a coluna 'LOCALIZAÇÃO' na posição desejada (por exemplo, após a coluna 'PRODUTO'), para união dos dfs
    insert_at = df_lgn.columns.get_loc(df_lgn.columns[-2]) + 1
    df_lgn.insert(insert_at, 'LOCALIZAÇÃO', 'NÃO SE APLICA')
    
    max_year = max(df_petroleo['ANO'].max(), df_gas['ANO'].max(), df_lgn['ANO'].max())
    min_year = min(df_petroleo['ANO'].min(), df_gas['ANO'].min(), df_lgn['ANO'].min())
    df_union = pd.concat([df_petroleo, df_gas, df_lgn], ignore_index=True).query('ANO in [@min_year, @max_year - 1, @max_year]')
    # ['ANO', 'MÊS', 'GRANDE REGIÃO', 'UNIDADE DA FEDERAÇÃO', 'PRODUTO', 'LOCALIZAÇÃO', 'PRODUÇÃO']
    cols = df_petroleo.columns.tolist()
    del cols[1]  # remove 'MÊS' para agrupo por ANO
    del cols[-2]  # remove 'LOCALIZAÇÃO' para agrupo por ANO
    # ['ANO', 'GRANDE REGIÃO', 'UNIDADE DA FEDERAÇÃO', 'PRODUTO', 'PRODUÇÃO']
    
    # df com todos os estados
    df = df_union.groupby(cols[:-1])[cols[-1]].sum().reset_index()

    # df com dados agrupados, formando o Brasil
    df_br = df.copy()
    df_br[cols[2]] = 'BR'
    df_br[cols[1]] = 'BRASIL'
    df_br = df_br.groupby(cols[:-1])[cols[-1]].sum().reset_index()

    # df com dados agrupados, formando o Nordeste
    df_ne = df.loc[df[cols[1]].str.lower().str.contains('nordeste')].copy()
    df_ne[cols[2]] = 'NE'
    df_ne = df_ne.groupby(cols[:-1])[cols[-1]].sum().reset_index()

    # df com dados de Sergipe
    df_se = df.loc[df[cols[2]] == 'SERGIPE'].copy()
    df_se[cols[2]] = 'SE'

    df_concat = pd.concat([df_br, df_ne, df_se], ignore_index=True)
    df_concat['min_year'] = df_concat.groupby([cols[2], cols[-2]])[cols[-1]].transform('first')
    df_concat['last_year'] = df_concat.groupby([cols[2], cols[-2]])[cols[-1]].shift(1)

    df_final = df_concat.loc[df_concat['ANO'] == max_year].copy()
    df_final['atual-ano anterior'] = ((df_final[cols[-1]] / df_final['last_year']) - 1) * 100
    df_final['atual/1997'] = ((df_final[cols[-1]] / df_final['min_year']) - 1) * 100
    final_cols = df_final.columns.tolist()
    
    # tratamento da coluna 'PRODUTO'
    df_final[cols[-2]] = df_final[cols[-2]].map({
        'PETRÓLEO': 'Petróleo',
        'GÁS NATURAL': 'Gás',
        'LGN': 'LGN'
    })
    df_final[cols[-2]] = df_final[cols[-2]] + '-' + df_final[cols[2]]
    df_final.rename(columns={cols[-2]: 'Produto'}, inplace=True)
    df_final.sort_values(by='Produto', inplace=True)
    df_final = df_final[['Produto'] + final_cols[-2:]]
    # df_final = df_final.melt(id_vars=['Produto'], var_name='Categoria', value_name='Valor')

    df_final.to_excel(os.path.join(sheets_path, 'g8.3.xlsx'), index=False, sheet_name='g8.3')

except Exception as e:
    errors['Gráfico 8.3'] = traceback.format_exc()


# g8.4
try:
    df_petroleo = c.open_file(dbs_path, 'anp_producao_petroleo.csv', 'csv', sep=';')
    df_gas = c.open_file(dbs_path, 'anp_producao_gas.csv', 'csv', sep=';')
    df_lgn = c.open_file(dbs_path, 'anp_producao_lgn.csv', 'csv', sep=';')
    # Insere a coluna 'LOCALIZAÇÃO' na posição desejada (por exemplo, após a coluna 'PRODUTO'), para união dos dfs
    insert_at = df_lgn.columns.get_loc(df_lgn.columns[-2]) + 1
    df_lgn.insert(insert_at, 'LOCALIZAÇÃO', 'NÃO SE APLICA')
    
    max_year = max(df_petroleo['ANO'].max(), df_gas['ANO'].max(), df_lgn['ANO'].max())
    min_year = min(df_petroleo['ANO'].min(), df_gas['ANO'].min(), df_lgn['ANO'].min())
    df_union = pd.concat([df_petroleo, df_gas, df_lgn], ignore_index=True)
    # ['ANO', 'MÊS', 'GRANDE REGIÃO', 'UNIDADE DA FEDERAÇÃO', 'PRODUTO', 'LOCALIZAÇÃO', 'PRODUÇÃO']
    cols = df_petroleo.columns.tolist()
    del cols[1]  # remove 'MÊS' para agrupo por ANO
    del cols[-2]  # remove 'LOCALIZAÇÃO' para agrupo por ANO
    # ['ANO', 'GRANDE REGIÃO', 'UNIDADE DA FEDERAÇÃO', 'PRODUTO', 'PRODUÇÃO']
    
    # df com todos os estados
    df = df_union.groupby(cols[:-1])[cols[-1]].sum().reset_index()

    # df com dados agrupados, formando o Brasil
    df_br = df.copy()
    df_br[cols[2]] = 'BR'
    df_br[cols[1]] = 'BRASIL'
    df_br = df_br.groupby(cols[:-1])[cols[-1]].sum().reset_index()

    # df com dados agrupados, formando o Nordeste
    df_ne = df.loc[df[cols[1]].str.lower().str.contains('nordeste')].copy()
    df_ne[cols[2]] = 'NE'
    df_ne = df_ne.groupby(cols[:-1])[cols[-1]].sum().reset_index()

    # df com dados de Sergipe
    df_se = df.loc[df[cols[2]] == 'SERGIPE', [cols[0], cols[-2], cols[-1]]].copy()
    df_se[cols[2]] = 'SE'

    df_concat = pd.concat([df_br, df_ne], ignore_index=True)
    df_joined = df_concat.merge(df_se, on=[cols[0], cols[-2]], how='left', suffixes=('', '_SE'), validate='m:1')
    df_joined['Razão'] = (df_joined[f'{cols[-1]}_SE'] / df_joined[cols[-1]]) * 100
    
    # tratamento da coluna 'PRODUTO'
    df_joined[cols[-2]] = df_joined[cols[-2]].map({
        'PETRÓLEO': 'Petróleo',
        'GÁS NATURAL': 'Gás',
        'LGN': 'LGN'
    })
    df_joined[cols[-2]] = df_joined[cols[-2]] + '-SE/' + df_joined[cols[2]]

    df_pivoted = df_joined.pivot(index=cols[0], columns=cols[-2], values='Razão').reset_index()
    df_final = df_pivoted[[cols[0]] + ['Petróleo-SE/BR', 'Gás-SE/BR', 'LGN-SE/BR', 'Petróleo-SE/NE', 'Gás-SE/NE', 'LGN-SE/NE']].copy()
    df_final.rename(columns={cols[0]: 'Ano'}, inplace=True)

    df_final.to_excel(os.path.join(sheets_path, 'g8.4.xlsx'), index=False, sheet_name='g8.4')

except Exception as e:
    errors['Gráfico 8.4'] = traceback.format_exc()


# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g8.1--g8.2--g8.3--g8.4.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
