"""
SCRIPT DE DOWNLOAD E CONSOLIDAÇÃO DE DADOS - PRODUÇÃO AGRÍCOLA MUNICIPAL PERMANENTE (PAM)

FONTE: IBGE - Sistema IBGE de Recuperação Automática (SIDRA)
TABELA: [INSERIR NÚMERO DA TABELA] - Produção agrícola municipal das lavouras permanentes

FLUXO DO SCRIPT:
1. Configuração inicial e criação de diretórios
2. Carregamento da base consolidada existente (se houver) para verificar anos já processados
3. Loop pelos anos (2010 até ano atual):
   - Verifica se o ano já foi processado (arquivo individual + base consolidada)
   - Faz download da API do IBGE se necessário
   - Processa e limpa os dados (tipos de dados, renomeação de colunas)
   - Salva arquivo individual em formato Parquet
   - Adiciona dados novos para consolidação posterior
4. Atualização da base consolidada com todos os dados novos em uma única operação
5. Relatório final do processamento

PRINCIPAIS OTIMIZAÇÕES:
- Verificação eficiente de anos já processados usando set()
- Consolidação em lote para evitar múltiplas operações de I/O
- Tratamento de erros para garantir continuidade do processamento
- Uso de tipos de dados otimizados (int16, float32, category)
"""

import functions as c
import os
import pandas as pd
from datetime import datetime


# obtém o caminho desse arquivo de comandos para adicionar os diretórios que armazenará as bases de dados e planilhas
raw_path = c.raw_path
pam_path = os.path.join(raw_path, 'producao_agricola_municipal_permanente')


# cria o diretório se não existir
os.makedirs(pam_path, exist_ok=True)


# ************************
# DOWNLOAD DA BASE DE DADOS
# ************************

# carrega a base consolidada existente (se houver) para verificar anos já processados
consolidado_path = os.path.join(pam_path, 'raw_producao_agricola_municipal_permanente_consolidado.parquet')
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
    file_name = f'raw_producao_agricola_municipal_permanente_{year}.parquet'
    file_path = os.path.join(pam_path, file_name)
    
    # verifica se precisa processar este ano
    if os.path.exists(file_path) and year in anos_existentes:
        print(f'Dados do ano {year} já processados. Pulando...')
        continue
    
    print(f'Processando dados do ano {year}...')
    
    try:
        # faz o download se o arquivo não existir
        if not os.path.exists(file_path):
            print(f'Baixando dados do ano {year}...')
            url = f'https://apisidra.ibge.gov.br/values/t/1613/n6/2801207,2802403,2805406,2805604,2800100,2800704,2801108,2802700,2804409,2804706,2804904,2805703,2807303,2800209,2801405,2801603,2804458,2801900,2802205,2802304,2803104,2803401,2803807,2804300,2804607,2805000,2805208,2806008,2806909,2801306,2801504,2802007,2802502,2803302,2803609,2804003,2805307,2805901,2806107,2806503,2806602,2807204,2800506,2801009,2802908,2803708,2803906,2804102,2806800,2800407,2800670,2803005,2803500,2805109,2805802,2806206,2807105,2800308,2800605,2802106,2802809,2803203,2804805,2806305,2806701,2807600,2801702,2805505,2807402,2807501,2806404/v/all/p/{year}/c82/allxt/d/v1000215%202,v1000216%202,v1002313%202'
            
            if not url.strip():
                print(f'AVISO: URL não configurada para o ano {year}. Configure a URL na linha 71.')
                print('Pulando para o próximo ano...')
                continue
                
            session = c.create_session_with_retries()
            response = session.get(url, timeout=session.request_timeout, headers=c.headers)
            response.raise_for_status()  # levanta exceção se houver erro HTTP
            
            data = pd.DataFrame(response.json())
            
            # tratamento dos dados
            data.columns = data.iloc[0]
            data = data.iloc[1:][['Unidade de Medida', 'Valor', 'Município', 'Variável', 'Ano', 'Produto das lavouras permanentes']]
            data['Ano'] = data['Ano'].astype('int16')
            data['Valor'] = pd.to_numeric(data['Valor'], errors='coerce').astype('float32')
            data['Município'] = data['Município'].astype('category')
            data['Unidade de Medida'] = data['Unidade de Medida'].astype('category')
            data['Variável'] = data['Variável'].astype('category')
            data['Produto das lavouras permanentes'] = data['Produto das lavouras permanentes'].astype('category')
            data.rename(columns={'Produto das lavouras permanentes': 'Produto'}, inplace=True)
            
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