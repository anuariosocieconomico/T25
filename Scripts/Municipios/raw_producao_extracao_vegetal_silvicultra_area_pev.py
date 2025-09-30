import functions as c
import os
import pandas as pd
from datetime import datetime
from time import sleep


# obtém o caminho desse arquivo de comandos para adicionar os diretórios que armazenará as bases de dados e planilhas
raw_path = c.raw_path
db_name = 'producao_extracao_vegetal_silvicultra_area_pev'  # nome base da base de dados; alterar conforme necessário e o valor será atualizado em todo o script
db_path = os.path.join(raw_path, db_name)


# cria o diretório se não existir
os.makedirs(db_path, exist_ok=True)


# ************************
# DOWNLOAD DA BASE DE DADOS
# ************************

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
    
    # verifica se precisa processar este ano
    if os.path.exists(file_path) and year in anos_existentes:
        print(f'Dados do ano {year} já processados. Pulando...')
        continue
    
    print(f'Processando dados do ano {year}...')
    
    try:
        # faz o download se o arquivo não existir
        if not os.path.exists(file_path):
            print(f'Baixando dados do ano {year}...')
            url = f'https://apisidra.ibge.gov.br/values/t/5930/n6/2804409,2804904,2805703,2803401,2803302,2806602,2800506,2802908,2800407,2803005,2803500,2805802,2806206,2802106,2802809,2803203,2804805,2806305,2806701,2807600,2801702,2807501,2806404/v/allxp/p/{year}/c734/allxt'
            session = c.create_session_with_retries()
            response = session.get(url, timeout=session.request_timeout, headers=c.headers)
            response.raise_for_status()  # levanta exceção se houver erro HTTP
            
            data = pd.DataFrame(response.json())
            
            # tratamento dos dados
            specific_columns = ['Espécie florestal']
            data.columns = data.iloc[0]
            data = data.iloc[1:][['Valor', 'Município', 'Variável', 'Ano'] + specific_columns]
            data['Ano'] = data['Ano'].astype('int16')
            data['Valor'] = pd.to_numeric(data['Valor'], errors='coerce', downcast='integer')
            data['Município'] = data['Município'].astype('category')
            data['Variável'] = data['Variável'].astype('category')
            # atenção neste ponto: aqui deve-se ajustar conforme a base que está sendo baixada
            data[specific_columns[0]] = data[specific_columns[0]].astype('category')
            data.rename(columns={specific_columns[0]: 'Produto'}, inplace=True)
            
            # salva o arquivo individual
            data.to_parquet(file_path, engine='pyarrow', compression='snappy', index=False)
            print(f'Arquivo {file_name} salvo com sucesso!')
        else:
            # carrega o arquivo existente
            data = pd.read_parquet(file_path)
            print(f'Arquivo {file_name} já existe, carregando...')
        
        # adiciona aos dados novos se ainda não está na base consolidada
        if year not in anos_existentes:
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

