import traceback
import functions as c
import os
import pandas as pd
from datetime import datetime, date
from time import sleep


# obtém o caminho desse arquivo de comandos para adicionar os diretórios que armazenará as bases de dados e planilhas
raw_path = c.raw_path
error_path = c.error_path
os.makedirs(error_path, exist_ok=True)
"""nome base da base de dados; alterar conforme necessário e o valor será atualizado em todo o script"""
db_name = 'pix'
db_path = os.path.join(raw_path, db_name)


# cria o diretório se não existir
os.makedirs(db_path, exist_ok=True)


# ************************
# DOWNLOAD DA BASE DE DADOS
# ************************

try:
    # Função para buscar dados PIX
    def get_pix_data(date_obj):
        date_str = date_obj.strftime("%Y%m")
        url = f"https://olinda.bcb.gov.br/olinda/servico/Pix_DadosAbertos/versao/v1/odata/TransacoesPixPorMunicipio(DataBase=@DataBase)?@DataBase='{date_str}'&$top=10000&$filter=Estado%20eq%20'SERGIPE'&$format=json&$select=AnoMes,Municipio_Ibge,Estado,VL_PagadorPF,QT_PagadorPF,VL_PagadorPJ,QT_PagadorPJ,VL_RecebedorPF,QT_RecebedorPF,VL_RecebedorPJ,QT_RecebedorPJ"
        session = c.create_session_with_retries()

        try:
            response = session.get(url, timeout=session.request_timeout, headers=c.headers)
            if response.status_code == 200:
                data = response.json()
                return pd.DataFrame(data['value'])
            else:
                print(f"Erro ao acessar os dados para {date_str}: Status {response.status_code}")
                return None
        except Exception as e:
            print(f"Erro na requisição para {date_str}: {e}")
            return None

    # carrega a base consolidada existente (se houver) para verificar anos já processados
    consolidado_path = os.path.join(db_path, 'raw_' + db_name + '_consolidado.parquet')
    anos_existentes = set()
    base_consolidada = None

    # se o arquivo consolidado existir, carrega os anos já processados
    if os.path.exists(consolidado_path):
        try:
            base_consolidada = pd.read_parquet(consolidado_path)
            anos_existentes = set(base_consolidada['AnoMes'].unique())
            print(f'Base consolidada encontrada com dados dos anos: {sorted(anos_existentes)}')
        except Exception as e:
            print(f'Erro ao carregar base consolidada: {e}')

    
    # Gerar sequência de datas
    start_date = datetime(2020, 11, 19).date()
    end_date = date.today().replace(day=1)  # Primeiro dia do mês atual

    # Gerar lista de primeiros dias de cada mês
    dates = pd.date_range(start=start_date, end=end_date, freq='MS').date

    # Buscar dados para todas as datas e combinar
    dados_novos = []
    for date_obj in dates:
        file_name = 'raw_' + db_name + f'_{date_obj.strftime("%Y-%m")}.parquet'
        file_path = os.path.join(db_path, file_name)
        
        print(f"Buscando dados para {date_obj.strftime('%Y-%m')}")
        try:
            data = get_pix_data(date_obj)

            # verifica se houve mudanças comparando com arquivo existente
            arquivo_atualizado = False
            if os.path.exists(file_path):
                data_existente = pd.read_parquet(file_path)
                # compara dimensões (linhas e colunas) e nomes das colunas
                if data.shape != data_existente.shape:
                    print(f'Diferença de dimensões detectada no ano {date_obj.strftime("%Y-%m")}: {data_existente.shape} -> {data.shape}')
                    arquivo_atualizado = True
                elif not data.columns.equals(data_existente.columns):
                    print(f'Diferença nos nomes das colunas detectada no ano {date_obj.strftime("%Y-%m")}')
                    arquivo_atualizado = True
                else:
                    print(f'Nenhuma mudança detectada no ano {date_obj.strftime("%Y-%m")}')
            else:
                print(f'Arquivo novo para o ano {date_obj.strftime("%Y-%m")}')
                arquivo_atualizado = True

            # salva apenas se houve mudança ou é arquivo novo
            if arquivo_atualizado:
                data.to_parquet(file_path, engine='pyarrow', compression='snappy', index=False)
                print(f'Arquivo {file_name} atualizado com sucesso!')
            else:
                # carrega o arquivo existente se não houve mudança
                data = pd.read_parquet(file_path)
            
            # adiciona aos dados novos se ainda não está na base consolidada ou se foi atualizado
            if date_obj.strftime('%Y-%m') not in anos_existentes or arquivo_atualizado:
                dados_novos.append(data)
                print(f'Dados do ano {date_obj.strftime("%Y-%m")} adicionados para consolidação')

        except Exception as e:
            print(f'Erro ao processar dados do ano {date_obj.strftime("%Y-%m")}: {e}')
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
    with open(os.path.join(error_path, 'log_raw_pix.txt'), 'w', encoding='utf-8') as f:
        f.write(f'Erro em raw_pix.py em {pd.Timestamp.now().strftime("%d/%m/%Y %H:%M:%S")}\n')
        f.write(error)
    print('Erro ao baixar ou processar os dados. Verifique o log em "Doc/Municipios/log_raw_pix.txt".')
