import traceback
import functions as c
import os
import pandas as pd
from datetime import datetime
from time import sleep


# obtém o caminho desse arquivo de comandos para adicionar os diretórios que armazenará as bases de dados e planilhas
raw_path = c.raw_path
error_path = c.error_path
os.makedirs(error_path, exist_ok=True)
db_name = 'pib'  # nome base da base de dados; alterar conforme necessário e o valor será atualizado em todo o script
db_path = os.path.join(raw_path, db_name)


# cria o diretório se não existir
os.makedirs(db_path, exist_ok=True)


# ************************
# DOWNLOAD DA BASE DE DADOS
# ************************

try:
    # carrega a base consolidada existente (se houver) para verificar anos já processados
    consolidado_path = os.path.join(db_path, 'raw_' + db_name + '_consolidado.parquet')
    anos_existentes = set()
    base_consolidada = None

    if os.path.exists(consolidado_path):
        try:
            base_consolidada = pd.read_parquet(consolidado_path)
            anos_existentes = set(base_consolidada['Ano'].unique())
            print(f'Base consolidada encontrada com dados dos anos: {sorted(anos_existentes)}')
        except Exception as e:
            print(f'Erro ao carregar base consolidada: {e}')

    dados_novos = []

    for year in range(2010, datetime.now().year + 1):
        file_name = 'raw_' + db_name + f'_{year}.parquet'
        file_path = os.path.join(db_path, file_name)
        
        print(f'Processando dados do ano {year}...')
        
        try:
            # sempre baixa os dados para verificar mudanças
            print(f'Baixando dados do ano {year}...')
            url = f'https://apisidra.ibge.gov.br/values/t/5938/n6/2801207,2802403,2804201,2804508,2805406,2805604,2800100,2800704,2801108,2802700,2804409,2804706,2804904,2805703,2807303,2800209,2801603,2801900,2802205,2802601,2803401,2804300,2804607,2806909,2801306,2802007,2803302,2803609,2804003,2805307,2805901,2806602,2807204,2800506,2801009,2806800,2800407,2800670,2803005,2805109,2805802,2806206,2800308,2800605,2802106,2802809,2803203,2804805,2806305,2806701,2807600,2801702,2807402,2807501,2806404/v/all/p/{year}'
            session = c.create_session_with_retries()
            response = session.get(url, timeout=session.request_timeout, headers=c.headers)
            response.raise_for_status()  # levanta exceção se houver erro HTTP
            
            data = pd.DataFrame(response.json())
            
            # tratamento dos dados

            data.columns = data.iloc[0]
            data = data.iloc[1:][['Unidade de Medida', 'Valor', 'Município', 'Variável', 'Ano']]
            data['Ano'] = data['Ano'].astype('int16')
            data['Valor'] = pd.to_numeric(data['Valor'], errors='coerce', downcast='integer')
            data['Município'] = data['Município'].astype('category')
            data['Variável'] = data['Variável'].astype('category')
            
            # verifica se houve mudanças comparando com arquivo existente
            arquivo_atualizado = False
            if os.path.exists(file_path):
                data_existente = pd.read_parquet(file_path)
                # compara dimensões (linhas e colunas) e nomes das colunas
                if data.shape != data_existente.shape:
                    print(f'Diferença de dimensões detectada no ano {year}: {data_existente.shape} -> {data.shape}')
                    arquivo_atualizado = True
                elif not data.columns.equals(data_existente.columns):
                    print(f'Diferença nos nomes das colunas detectada no ano {year}')
                    arquivo_atualizado = True
                else:
                    print(f'Nenhuma mudança detectada no ano {year}')
            else:
                print(f'Arquivo novo para o ano {year}')
                arquivo_atualizado = True
            
            # salva apenas se houve mudança ou é arquivo novo
            if arquivo_atualizado:
                data.to_parquet(file_path, engine='pyarrow', compression='snappy', index=False)
                print(f'Arquivo {file_name} atualizado com sucesso!')
            else:
                # carrega o arquivo existente se não houve mudança
                data = pd.read_parquet(file_path)
            
            # adiciona aos dados novos se ainda não está na base consolidada ou se foi atualizado
            if year not in anos_existentes or arquivo_atualizado:
                dados_novos.append(data)
                print(f'Dados do ano {year} adicionados para consolidação')
                
        except Exception as e:
            print(f'Erro ao processar dados do ano {year}: {e}')
            continue

    # atualiza a base consolidada se houver dados novos
    if dados_novos:
        print('Atualizando base consolidada...')
        try:
            if base_consolidada is not None:
                nova_base = pd.concat([base_consolidada] + dados_novos, ignore_index=True)
            else:
                nova_base = pd.concat(dados_novos, ignore_index=True)
            
            nova_base.to_parquet(consolidado_path, engine='pyarrow', compression='snappy', index=False)
            print(f'Base consolidada atualizada com {len(dados_novos)} novos anos!')
        except Exception as e:
            print(f'Erro ao salvar base consolidada: {e}')
    else:
        print('Nenhum dado novo para consolidar.')

    print('Processamento concluído!')

except:
    error = traceback.format_exc()
    with open(os.path.join(error_path, 'log_raw_pib.txt'), 'w', encoding='utf-8') as f:
        f.write(f'Erro em raw_pib.py em {pd.Timestamp.now().strftime("%d/%m/%Y %H:%M:%S")}\n')
        f.write(error)
    print('Erro ao baixar ou processar os dados. Verifique o log em "Doc/Municipios/log_raw_pib.txt".')
