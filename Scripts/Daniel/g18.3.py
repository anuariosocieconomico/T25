import functions as c
import os
import pandas as pd
import json
import traceback
import tempfile
import shutil
import ipeadatapy
from io import StringIO
from bs4 import BeautifulSoup


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
url = 'http://www.ans.gov.br/anstabnet/cgi-bin/dh?dados/tabnet_br.def'

# tenta baixar o arquivo conforme os comandos anteriores; caso haja alguma atualização no site, registra-se o erro
try:
    driver = c.Google(visible=False, rep=dbs_path)  # instância do objeto driver do Selenium
    driver.get(url)  # acessa a página

    driver.wait('/html/body/table[2]/thead/tr[2]/td[2]/center/form/select[1]/option[11]')
    # seleciona as opções da tabela
    driver.click([
        '/html/body/table[2]/thead/tr[2]/td[2]/center/form/select[1]/option[11]',  # linha UF
        '/html/body/table[2]/thead/tr[2]/td[2]/center/form/select[2]/option[2]',  # coluna Competência
        '/html/body/table[2]/thead/tr[2]/td[2]/center/form/select[3]/option[1]',  # conteúdo Assistência Médica
    ])

    # seleciona todos os períodos disponíveis
    driver.periods('select', 'A', 'option', all_periods=False, prefix='Dez')

    # abre a tabela
    driver.click([
        '/html/body/table[2]/thead/tr[2]/td[2]/center/form/table[2]/tbody/tr[15]/td/input',  # checkbox para ordenar valores
        '/html/body/table[2]/thead/tr[2]/td[2]/center/form/table[2]/tbody/tr[15]/td/p[2]/input[1]'  # botçao Mostrar
    ])

    # altera para a última aba aberta e baixa o arquivo
    driver.change_window()
    driver.wait('/html/body/table[2]/thead/tr[2]/td[2]/center/table/tbody')

    # extrai a tabela da html (ao baixar diretamente no botão, a tabela retornada era incompleta)
    html = driver.browser.page_source
    soup = BeautifulSoup(html, 'html.parser')
    df = pd.read_html(StringIO(str(soup)), skiprows=1, thousands='.')[0]
    df = df.iloc[:-1]
    for col in df.columns[1:]:
        df[col] = df[col].astype(int)
    c.to_csv(df, dbs_path, 'tabnet.csv')
    file_name = 'tabnet.csv'
    
    driver.quit()

# registro do erro em caso de atualizações
except Exception as e:
    errors[url] = traceback.format_exc()


# sidra 7358
url = 'https://apisidra.ibge.gov.br/values/t/7358/n1/all/n2/2/n3/28/v/all/p/all/c2/6794/c287/100362/c1933/all?formato=json'
try:
    data = c.open_url(url)
    df = pd.DataFrame(data.json())
    df = df[['D6N', 'D1N', 'V']].copy()
    df.columns = ['Ano', 'Região', 'Valor']
    df.drop(0, axis='index', inplace=True)  # remove a primeira linha que contém o cabeçalho
    df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')  # converte a coluna Valor para numérico, tratando erros
    df['Ano'] = df['Ano'].astype(int)

    c.to_excel(df, dbs_path, 'sidra_7358.xlsx')
except Exception as e:
    errors['Sidra 7358'] = traceback.format_exc()


# ************************
# PLANILHA
# ************************

# g18.3
try:
    df = c.open_file(dbs_path, 'tabnet.csv', 'csv').query('UF.str.lower() != "exterior" and UF.str.lower() != "não identificado"', engine='python')
    pop = c.open_file(dbs_path, 'sidra_7358.xlsx', 'xls', sheet_name='Sheet1')
    pop.rename(columns={'Região': 'UF'}, inplace=True)

    df_melted = df.melt(id_vars='UF', value_vars=df.columns[1:], var_name='Ano', value_name='Valor')
    df_melted['Ano'] = '20' + df_melted['Ano'].str.split('/').str[-1]
    df_melted['Ano'] = df_melted['Ano'].astype(int)
    df_melted.loc[df_melted['UF'].str.lower() == 'total', 'UF'] = 'Brasil'  # renomeia a unidade da federação Total para Brasil

    df_ne = df_melted.loc[df_melted['UF'].isin(c.ne_states)].copy()
    df_ne.loc[:, 'UF'] = 'Nordeste'  # renomeia a unidade da federação Total para Brasil
    df_ne = df_ne.groupby(['UF', 'Ano']).sum().reset_index()  # soma os estados do Nordeste

    df_concat = pd.concat([df_melted.query('UF in ["Brasil", "Sergipe"]'), df_ne], ignore_index=True)
    df_merged = pd.merge(df_concat, pop, on=['UF', 'Ano'], how='inner', validate='1:1', suffixes=['_benef', '_pop'])  # mescla as tabelas
    df_merged['Proporação'] = (df_merged['Valor_benef'] / df_merged['Valor_pop']) * 100  # calcula a proporção de beneficiários por população

    df_pivoted = df_merged.pivot(index='Ano', columns='UF', values='Proporação').reset_index()

    df_pivoted.to_excel(os.path.join(sheets_path, 'g18.3.xlsx'), index=False, sheet_name='g18.3')

except Exception as e:
    errors['Gráfico 18.3'] = traceback.format_exc()


# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g18.3.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
