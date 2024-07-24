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

# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g8.1--g8.2.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
