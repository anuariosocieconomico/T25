import functions as c
import os
import pandas as pd
import numpy as np
import json
import traceback
import tempfile
import shutil
import sidrapy
import ipeadatapy
from datetime import datetime
import time
import re
import requests


# pandas config
pd.set_option('display.float_format', lambda x: '{:,.4f}'.format(x))  # mostra os números com 4 casas decimais e separador de milhar

# obtém o caminho desse arquivo de comandos para adicionar os diretórios que armazenará as bases de dados e planilhas
dbs_path = tempfile.mkdtemp()
sheets_path = c.sheets_dir
errors_path = c.errors_dir

# inicializa o dicionário para armazenar informações sobre possíveis erros durante a execução
errors = {}


# ************************
# DOWNLOAD DA BASE DE DADOS
# ************************

session = c.create_session_with_retries()
# deflator IPEA IPCA
try:
    data = ipeadatapy.timeseries('PRECOS_IPCAG')
    data.rename(columns={'YEAR': 'Ano', 'VALUE ((% a.a.))': 'Valor'}, inplace=True)  # renomeia as colunas
    c.to_excel(data, dbs_path, 'ipeadata_ipca.xlsx')
    print('IPEA IPCA baixado com sucesso!')
except Exception as e:
    errors['IPEA IPCA'] = traceback.format_exc()


# sidra 1187 - estimativa da população
url = 'https://apisidra.ibge.gov.br/values/t/1187/n1/all/n2/2/n3/28/v/all/p/all/c2/6794/d/v2513%201?formato=json'
try:
    data = session.get(url, timeout=session.request_timeout, headers=c.headers)
    df = pd.DataFrame(data.json())
    df = df[['D3N', 'D1N', 'V']].copy()
    df.columns = ['Ano', 'Região', 'Valor']
    df.drop(0, axis='index', inplace=True)  # remove a primeira linha que contém o cabeçalho
    df['Ano'] = df['Ano'].astype(int)
    df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')  # converte a coluna Valor para numérico, tratando erros

    c.to_excel(df, dbs_path, 'sidra_1187.xlsx')
    print('Sidra 1187 baixado com sucesso!')
except Exception as e:
    errors['Sidra 1187'] = traceback.format_exc()


# sidra 7113 - estimativa da população
url = 'https://apisidra.ibge.gov.br/values/t/7113/n1/all/n2/2/n3/28/v/10267/p/all/c2/6794/c58/2795/d/v10267%201?formato=json'
try:
    data = session.get(url, timeout=session.request_timeout, headers=c.headers)
    df = pd.DataFrame(data.json())
    df = df[['D3N', 'D1N', 'V']].copy()
    df.columns = ['Ano', 'Região', 'Valor']
    df.drop(0, axis='index', inplace=True)  # remove a primeira linha que contém o cabeçalho
    df['Ano'] = df['Ano'].astype(int)
    df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')  # converte a coluna Valor para numérico, tratando erros

    c.to_excel(df, dbs_path, 'sidra_7113.xlsx')
    print('Sidra 7113 baixado com sucesso!')
except Exception as e:
    errors['Sidra 7113'] = traceback.format_exc()


# sidra 356 - estimativa da população
url = 'https://apisidra.ibge.gov.br/values/t/356/n1/all/n2/2/n3/28/v/1887/p/all/c1/6795/c2/6794/c58/2795/d/v1887%201?formato=json'
try:
    data = session.get(url, timeout=session.request_timeout, headers=c.headers)
    df = pd.DataFrame(data.json())
    df = df[['D3N', 'D1N', 'V']].copy()
    df.columns = ['Ano', 'Região', 'Valor']
    df.drop(0, axis='index', inplace=True)  # remove a primeira linha que contém o cabeçalho
    df['Ano'] = df['Ano'].astype(int)
    df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')  # converte a coluna Valor para numérico, tratando erros

    c.to_excel(df, dbs_path, 'sidra_356.xlsx')
    print('Sidra 356 baixado com sucesso!')
except Exception as e:
    errors['Sidra 356'] = traceback.format_exc()


# sidra 7126 - estimativa da população
url = 'https://apisidra.ibge.gov.br/values/t/7126/n1/all/n2/2/n3/28/v/3593/p/all/c2/6794/c58/2795/d/v3593%201?formato=json'
try:
    data = session.get(url, timeout=session.request_timeout, headers=c.headers)
    df = pd.DataFrame(data.json())
    df = df[['D3N', 'D1N', 'V']].copy()
    df.columns = ['Ano', 'Região', 'Valor']
    df.drop(0, axis='index', inplace=True)  # remove a primeira linha que contém o cabeçalho
    df['Ano'] = df['Ano'].astype(int)
    df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')  # converte a coluna Valor para numérico, tratando erros

    c.to_excel(df, dbs_path, 'sidra_7126.xlsx')
    print('Sidra 7126 baixado com sucesso!')
except Exception as e:
    errors['Sidra 7126'] = traceback.format_exc()


# sidra 7143 - estimativa da população
url = 'https://apisidra.ibge.gov.br/values/t/7143/n1/all/n2/2/n3/28/v/10274/p/all/c872/47821,47823,47825,47826/c11797/allxt/d/v10274%201?formato=json'
try:
    data =session.get(url, timeout=session.request_timeout, headers=c.headers)
    df = pd.DataFrame(data.json())
    df = df[['D3N', 'D1N', 'D5N', 'D4N', 'V']].copy()
    df.columns = ['Ano', 'Região', 'Rede', 'Nível', 'Valor']
    df.drop(0, axis='index', inplace=True)  # remove a primeira linha que contém o cabeçalho
    df['Ano'] = df['Ano'].astype(int)
    df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')  # converte a coluna Valor para numérico, tratando erros

    c.to_excel(df, dbs_path, 'sidra_7143.xlsx')
    print('Sidra 7143 baixado com sucesso!')
except Exception as e:
    errors['Sidra 7143'] = traceback.format_exc()


# ideb resultados anos iniciais
url = 'https://www.gov.br/inep/pt-br/areas-de-atuacao/pesquisas-estatisticas-e-indicadores/ideb/resultados'
try:
    driver = c.Google(visible=False, rep=dbs_path)  # instância do objeto driver do Selenium
    driver.get(url)  # acessa a página
    time.sleep(2)
    driver.random_click()
    driver.click('/html/body/div[5]/div/div/div/div/div[2]/button[2]')  # aceita os cookies
    driver.wait('/html/body/div[2]/div[1]/main/div[2]/div/div[4]/div[2]/div/div/ul[4]/li/a')
    driver.click('/html/body/div[2]/div[1]/main/div[2]/div/div[4]/div[2]/div/div/ul[4]/li/a')  # clica no link de download da planilha
    time.sleep(2)

    files = os.listdir(dbs_path)
    print("Arquivos encontrados:", files)

    for file in files:
        print("Tamanho do arquivo:", os.path.getsize(os.path.join(dbs_path, file)))

        with open(os.path.join(dbs_path, file), 'rb') as f:
            print("Primeiros bytes:", f.read(100))

    file_name = [file for file in files if '.zip' in file.lower()][0]
    print('Arquivo escolhido:', file_name)
    # abre o arquivo zip
    data = c.open_file(dbs_path, file_name, ext='zip', excel_name='divulgacao', skiprows=9)
    df_tb = data[list(data.keys())[0]].copy()  # cópia da primeira aba da planilha
    index = df_tb[df_tb[df_tb.columns[0]].str.lower().str.contains('fonte', na=False)].index  # localiza a linha de fonte para remover tudo que vem depois dela
    df_tb = df_tb.iloc[:index[0], :].copy()  # mantém apenas as linhas até a linha de fonte
    df_tb.dropna(how='all', inplace=True)  # remove linhas que estão completamente vazias
    cols = [col for col in df_tb.columns if 'Unnamed' in col or 'VL_OBSERVADO' in col or 'VL_PROJECAO' in col]

    df_filtered = df_tb[cols].copy()  # mantém apenas as colunas de interesse
    df_filtered.rename(columns={cols[0]: 'Região', cols[1]: 'Rede'}, inplace=True)  # renomeia as colunas
    df_melted = df_filtered.melt(id_vars=['Região', 'Rede'], var_name='Ano', value_name='Valor')  # transforma a tabela para o formato longo
    df_melted['Categoria'] = df_melted['Ano'].str.split('_').str[1]  # extrai a categoria (observado ou projeção) da coluna Ano
    df_melted['Ano'] = df_melted['Ano'].str.split('_').str[-1].astype(int)  # extrai o ano da coluna Ano
    df_melted['Valor'] = pd.to_numeric(df_melted['Valor'], errors='coerce')  # converte a coluna Valor para numérico, tratando erros

    c.to_excel(df_melted, dbs_path, 'ideb_anos_iniciais.xlsx')
    print('IDEB Anos Iniciais baixado com sucesso!')
    # driver.quit()  # encerra o driver do Selenium

except Exception as e:
    errors['IDEB - Anos Iniciais'] = traceback.format_exc()


# ideb resultados anos iniciais
url = 'https://www.gov.br/inep/pt-br/areas-de-atuacao/pesquisas-estatisticas-e-indicadores/ideb/resultados'
try:
    # driver = c.Google(visible=False, rep=dbs_path)  # instância do objeto driver do Selenium
    driver.get(url)  # acessa a página
    time.sleep(2)
    driver.random_click()
    # driver.click('/html/body/div[5]/div/div/div/div/div[2]/button[2]')  # aceita os cookies
    # driver.wait('/html/body/div[2]/div[1]/main/div[2]/div/div[4]/div[2]/div/div/ul[4]/li/a')
    driver.click('/html/body/div[2]/div[1]/main/div[2]/div/div[4]/div[2]/div/div/ul[4]/li/a')  # clica no link de download da planilha
    time.sleep(2)

    files = os.listdir(dbs_path)
    print("Arquivos encontrados:", files)

    for file in files:
        print("Tamanho do arquivo:", os.path.getsize(os.path.join(dbs_path, file)))

        with open(os.path.join(dbs_path, file), 'rb') as f:
            print("Primeiros bytes:", f.read(100))

    # abre o arquivo zip
    file_name = [file for file in files if '.zip' in file.lower() and file != 'ideb_anos_iniciais.xlsx'][0]
    print('Arquivo escolhido:', file_name)
    dfs = []
    data = c.open_file(dbs_path, file_name, ext='zip', excel_name='divulgacao', skiprows=9)
    for tb in list(data.keys()):
        df_tb = data[tb].copy()  # cópia da primeira aba da planilha
        index = df_tb[df_tb[df_tb.columns[0]].str.lower().str.contains('fonte', na=False)].index  # localiza a linha de fonte para remover tudo que vem depois dela
        df_tb = df_tb.iloc[:index[0], :].copy()  # mantém apenas as linhas até a linha de fonte
        df_tb.dropna(how='all', inplace=True)  # remove linhas que estão completamente vazias
        cols = [col for col in df_tb.columns if 'Unnamed' in col or 'VL_OBSERVADO' in col or 'VL_PROJECAO' in col]

        df_filtered = df_tb[cols].copy()  # mantém apenas as colunas de interesse
        df_filtered.rename(columns={cols[0]: 'Região', cols[1]: 'Rede'}, inplace=True)  # renomeia as colunas
        df_melted = df_filtered.melt(id_vars=['Região', 'Rede'], var_name='Ano', value_name='Valor')  # transforma a tabela para o formato longo
        df_melted['Categoria'] = df_melted['Ano'].str.split('_').str[1]  # extrai a categoria (observado ou projeção) da coluna Ano
        df_melted['Ano'] = df_melted['Ano'].str.split('_').str[-1].astype(int)  # extrai o ano da coluna Ano
        df_melted['Valor'] = pd.to_numeric(df_melted['Valor'], errors='coerce')  # converte a coluna Valor para numérico, tratando erros
        df_melted['Série'] = 'Anos Iniciais' if '(AI)' in tb else 'Anos Finais' if '(AF)' in tb else 'Ensino Médio'  # adiciona uma coluna para identificar a série (Anos Iniciais, Anos Finais ou Médio)

        dfs.append(df_melted)

    df_final = pd.concat(dfs, ignore_index=True)
    c.to_excel(df_final, dbs_path, 'ideb_resultados.xlsx')
    print('IDEB Resultados baixado com sucesso!')
    driver.quit()  # encerra o driver do Selenium
    
except Exception as e:
    errors['IDEB Resultados'] = traceback.format_exc()



# ************************
# PLANILHA
# ************************

# gráfico 17.1
try:
    # tabela ibge
    data = c.open_file(dbs_path, 'sidra_1187.xlsx', 'xls', sheet_name='Sheet1')
    data['Valor'] = 100 - data['Valor']
    df_analfabetismo = c.open_file(dbs_path, 'sidra_7113.xlsx', 'xls', sheet_name='Sheet1')
    
    df = pd.concat([data, df_analfabetismo], ignore_index=True)

    df['Ano'] = '01/01/' + df['Ano'].astype(str)  # formata a coluna Ano para o formato de data
    df.rename(columns={'Valor': 'Taxa de analfabetismo'}, inplace=True)
    df.sort_values(by=['Região', 'Ano'], inplace=True)  # ordena os dados por Região e Ano

    df_final = df[['Região', 'Ano', 'Taxa de analfabetismo']].copy()

    c.to_excel(df_final, sheets_path, 'g17.1.xlsx')
    print('Gráfico 17.1 modelado com sucesso!')

except Exception as e:
    errors['Gráfico 17.1'] = traceback.format_exc()


# gráfico 17.2
try:
    # tabela ibge
    data = c.open_file(dbs_path, 'sidra_356.xlsx', 'xls', sheet_name='Sheet1')
    data_new = c.open_file(dbs_path, 'sidra_7126.xlsx', 'xls', sheet_name='Sheet1')
    
    df = pd.concat([data, data_new], ignore_index=True)

    df['Ano'] = '01/01/' + df['Ano'].astype(str)  # formata a coluna Ano para o formato de data
    df.sort_values(by=['Região', 'Ano'], inplace=True)  # ordena os dados por Região e Ano

    df_final = df[['Região', 'Ano', 'Valor']].copy()

    c.to_excel(df_final, sheets_path, 'g17.2.xlsx')
    print('Gráfico 17.2 modelado com sucesso!')

except Exception as e:
    errors['Gráfico 17.2'] = traceback.format_exc()


# tabela 17.1
try:
    # tabela ibge
    data = c.open_file(dbs_path, 'sidra_7143.xlsx', 'xls', sheet_name='Sheet1')

    data['Ano'] = '01/01/' + data['Ano'].astype(str)  # formata a coluna Ano para o formato de data
    data.sort_values(by=['Região', 'Ano', 'Nível', 'Rede'], inplace=True)  # ordena os dados por Região e Ano
    data.rename(columns={'Rede': 'Rede de ensino', 'Nível': 'Curso frequentado'}, inplace=True)

    df_final = data[['Região', 'Curso frequentado', 'Rede de ensino', 'Ano', 'Valor']].copy()

    c.to_excel(df_final, sheets_path, 't17.1.xlsx')
    print('Tabela 17.1 modelada com sucesso!')

except Exception as e:
    errors['Tabela 17.1'] = traceback.format_exc()


# gráfico 17.3
try:
    # tabela ibge
    data = c.open_file(dbs_path, 'ideb_resultados.xlsx', 'xls', sheet_name='Sheet1').query(
        'not `Região` in ["Norte", "Nordeste", "Sudeste", "Sul", "Centro-Oeste"] and ' \
        'Rede.str.contains("Total") and' \
        '`Série` == "Anos Iniciais"'
    )
    df_pivoted = pd.pivot(data, index=['Região', 'Ano'], columns='Categoria', values='Valor').reset_index()
    df_pivoted.sort_values(by=['Ano', 'Região'], inplace=True)
    df_pivoted['Rank'] = df_pivoted.groupby('Ano')['OBSERVADO'].rank(ascending=False, method='first')

    df_final = df_pivoted.query('`Região` == "Sergipe"')[['Ano', 'OBSERVADO', 'PROJECAO', 'Rank']].copy()
    df_final.rename(columns={'OBSERVADO': 'Resultado', 'PROJECAO': 'Meta', 'Rank': 'Ranking nacional'}, inplace=True)

    df_final.to_excel(os.path.join(sheets_path, 'g17.3.xlsx'), index=False, sheet_name='g17.3')
    print('Gráfico 17.3 modelado com sucesso!')

except Exception as e:
    errors['Gráfico 17.3'] = traceback.format_exc()


# gráfico 17.4
try:
    # tabela ibge
    data = c.open_file(dbs_path, 'ideb_resultados.xlsx', 'xls', sheet_name='Sheet1').query(
        'not `Região` in ["Norte", "Nordeste", "Sudeste", "Sul", "Centro-Oeste"] and ' \
        'Rede.str.contains("Total") and' \
        '`Série` == "Ensino Médio"'
    )
    df_pivoted = pd.pivot(data, index=['Região', 'Ano'], columns='Categoria', values='Valor').reset_index()
    df_pivoted.sort_values(by=['Ano', 'Região'], inplace=True)
    df_pivoted['Rank'] = df_pivoted.groupby('Ano')['OBSERVADO'].rank(ascending=False, method='first')

    df_final = df_pivoted.query('`Região` == "Sergipe"')[['Ano', 'OBSERVADO', 'PROJECAO', 'Rank']].copy()
    df_final.rename(columns={'OBSERVADO': 'Resultado', 'PROJECAO': 'Meta', 'Rank': 'Ranking nacional'}, inplace=True)

    df_final.to_excel(os.path.join(sheets_path, 'g17.4.xlsx'), index=False, sheet_name='g17.4')
    print('Gráfico 17.4 modelado com sucesso!')

except Exception as e:
    errors['Gráfico 17.4'] = traceback.format_exc()


# tabela 17.2
try:
    # tabela ibge
    data = c.open_file(dbs_path, 'ideb_resultados.xlsx', 'xls', sheet_name='Sheet1').query('`Região` == "Sergipe"')
    data['Rede'] = data['Rede'].str.split(' ').str[0]  # mantém apenas a primeira palavra da coluna Rede
    data.loc[data['Série'] == 'Anos Iniciais', 'Série'] = 'Fundamental - Anos Iniciais' # renomeia a série
    data.loc[data['Série'] == 'Anos Finais', 'Série'] = 'Fundamental - Anos Finais' # renomeia a série
    data['Categoria'] = data['Categoria'].map({'OBSERVADO': 'IDEB', 'PROJECAO': 'Projeção'})  # renomeia as categorias
    data.rename(columns={'Categoria': 'Classe'}, inplace=True)

    df_final = data[['Ano', 'Região', 'Série', 'Rede', 'Classe', 'Valor']].copy()
    df_final.sort_values(by=['Ano', 'Classe', 'Série', 'Rede'], inplace=True)
    df_final['Ano'] = '01/01/' + df_final['Ano'].astype(str)  # formata a coluna Ano para o formato de data

    c.to_excel(df_final, sheets_path, 't17.2.xlsx')
    print('Tabela 17.2 modelada com sucesso!')

except Exception as e:
    errors['Tabela 17.2'] = traceback.format_exc()


# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g17.1--g17.2--g17.3--g17.4--t17.1--t17.2.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
