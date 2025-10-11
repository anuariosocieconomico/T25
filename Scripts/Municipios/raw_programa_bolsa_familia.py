import traceback
import functions as c
import os
import pandas as pd
from time import sleep


# obtém o caminho desse arquivo de comandos para armazenar diretamente no diretório raw
raw_path = c.raw_path
error_path = c.error_path
os.makedirs(error_path, exist_ok=True)
os.makedirs(raw_path, exist_ok=True)
db_name = 'programa_bolsa_familia'  # nome base da base de dados
file_name = 'raw_' + db_name + '.parquet'
file_path = os.path.join(raw_path, file_name)


# ************************
# DOWNLOAD DA BASE DE DADOS
# ************************

try:
    print('Iniciando download dos dados do programa bolsa família...')

    # URL e headers para a API
    url = "https://aplicacoes.cidadania.gov.br/vis/data3/v.php?q[]=r6JtZJCug7BtxKW25rV%2FfmdhhJFkl21kmK19ZnB1ZXGmaX7KmZO20qfOnJm%2B6IianbSon7SfrrqqkpKcmcuppsK2iKextVi1mpyuwZxNzsmY2F1zyuDAk522pHa2YH9%2BaV6EkmOXbWSEm8GcobZVetufrL%2BrkbbHlNddmMnuslSqvaGmmZ67sliqkseU1rCYmOGuoK%2BtcHXfmrnBnGiS1KjXYK5%2B3q6noWisot6nbY6kksrAlNiscZqif2Rue2JqrGZ9f15Ny8mY2F1zv%2BGspbCslKDapm2zo6C8gaHfqZ994LuYXcVwoNqlwLNyk7jNps94bsPcuaehg3Ct7qZwyViQuNSYirSbwultdKmtqJnap7yKdFSJkWWbamSNqH1lY2ipot6nbY6Zk7bXn4qin9DgbaKxtKFa3qexb7RovcKf3aJuw9y5p6GDcKDapcCzcmjK1qCNuFTA3MCZXL%2Bdn%2BdZjbucoLbCodl7cIStfWZvdWVtpml%2BdVehv8ahin2Vw9rDoFytoa3eWbvDo5l3xqHOXrCY4a6gr61woNqlwLNyaL3Cn92ibpjuwqFfw1ad2qyybq6VvM9TqqqY0NquoquEcmGraX9%2FZF6HjmObZFPR47KiXHCYm%2ByebcWfksWBc8yjks7vsZOiqaJ4qVnBtpybd9Oi36uXhbuvmpu%2BoXSzp8K7nJ%2FAxGKqn5m87MGYm66Wp6Vrdm6cmcrGU9iyn8mbsqKgcVWf5ayybqWiw81Tz6uXfviImp20qJ%2B0n666qpKSnJnLqabCtoinsbVYtZqcrsGcTc7JmNhdc8rgwJOdtqR4tmB%2FfmlghJFml21khJvBnKG2VWLcmsCzV6S%2FxqGKfamPsYFnenhVruGeu26pnMzPl5J9lcPaw6B2gqOv5p6%2Ft5pcl9dloHFmia12VKG0qJ%2BZp8K6o028z5eTXZjJ7rJUqr2hppmeu7JYqpLHlNawmJjhrqCvrXB135q5wZxoktSo17l5vugQ4aixlq2Ze7K8nJPAxJwt3qXG3MBXgqmi%2FSaltq%2BqTaejeYpllNE%2B9lSLvalpq2l%2Ff2BQncKgLeqfxtzAVIyKe1qhmm2%2BmJ%2FLyqWKoZh9yK6ma3plbKxicKSYmcbTU9yio77uwJWgt1X9GaxttJiaGg6f056mfcuPelxwlq484m2drKGGk2OcblyA0a6gq7pVrN6prsGqjrvQUy3dpn3hrqH%2F9aGj2qxtnnlzd4mUiq2Uz%2B%2B2plysmlrGmr99aV2JlFyNk5TJ6r9UoLdVfN6nsrT62rrKooqq9gbftqNccJauPOJtnayhhpNjnG5cgNGuoKu6VZ7oWY%2BzpZK9JODNpqJ96BDdoLGkWqGabb6Yn8vKpYqhmH3IrqZremVsrGLJvnKp091lmm1miqt%2BYWx5iWqpc31%2BcV2Hu24%3D&ag=e&sag=28&codigo=0&wt=json&tp_funcao_consulta=0&rqprocess=260a978269caf430013c8badb7782e16"

    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "DNT": "1",
        "Origin": "https://aplicacoes.cidadania.gov.br",
        "Referer": "https://aplicacoes.cidadania.gov.br/vis/data3/v.php?q[]=r6JtZJCug7BtxKW25rV%2FfmdhhJFkl21kmK19ZnB1ZXGmaX7KmZO20qfOnJm%2B6IianbSon7SfrrqqkpKcmcuppsK2iKextVi1mpyuwZxNzsmY2F1zyuDAk522pHa2YH9%2BaV6EkmOXbWSEm8GcobZVetufrL%2BrkbbHlNddmMnuslSqvaGmmZ67sliqkseU1rCYmOGuoK%2BtcHXfmrnBnGiS1KjXYK5%2B3q6noWisot6nbY6kksrAlNiscZqif2Rue2JqrGZ9f15Ny8mY2F1zv%2BGspbCslKDapm2zo6C8gaHfqZ994LuYXcVwoNqlwLNyk7jNps94bsPcuaehg3Ct7qZwyViQuNSYirSbwultdKmtqJnap7yKdFSJkWWbamSNqH1lY2ipot6nbY6Zk7bXn4qin9DgbaKxtKFa3qexb7RovcKf3aJuw9y5p6GDcKDapcCzcmjK1qCNuFTA3MCZXL%2Bdn%2BdZjbucoLbCodl7cIStfWZvdWVtpml%2BdVehv8ahin2Vw9rDoFytoa3eWbvDo5l3xqHOXrCY4a6gr61woNqlwLNyaL3Cn92ibpjuwqFfw1ad2qyybq6VvM9TqqqY0NquoquEcmGraX9%2FZF6HjmObZFPR47KiXHCYm%2ByebcWfksWBc8yjks7vsZOiqaJ4qVnBtpybd9Oi36uXhbuvmpu%2BoXSzp8K7nJ%2FAxGKqn5m87MGYm66Wp6Vrdm6cmcrGU9iyn8mbsqKgcVWf5ayybqWiw81Tz6uXfviImp20qJ%2B0n666qpKSnJnLqabCtoinsbVYtZqcrsGcTc7JmNhdc8rgwJOdtqR4tmB%2FfmlghJFml21khJvBnKG2VWLcmsCzV6S%2FxqGKfamPsYFnenhVruGeu26pnMzPl5J9lcPaw6B2gqOv5p6%2Ft5pcl9dloHFmia12VKG0qJ%2BZp8K6o028z5eTXZjJ7rJUqr2hppmeu7JYqpLHlNawmJjhrqCvrXB135q5wZxoktSo17l5vugQ4aixlq2Ze7K8nJPAxJwt3qXG3MBXgqmi%2FSaltq%2BqTaejeYpllNE%2B9lSLvalpq2l%2Ff2BQncKgLeqfxtzAVIyKe1qhmm2%2BmJ%2FLyqWKoZh9yK6ma3plbKxicKSYmcbTU9yio77uwJWgt1X9GaxttJiaGg6f056mfcuPelxwlq484m2drKGGk2OcblyA0a6gq7pVrN6prsGqjrvQUy3dpn3hrqH%2F9aGj2qxtnnlzd4mUiq2Uz%2B%2B2plysmlrGmr99aV2JlFyNk5TJ6r9UoLdVfN6nsrT62rrKooqq9gbftqNccJauPOJtnayhhpNjnG5cgNGuoKu6VZ7oWY%2BzpZK9JODNpqJ96BDdoLGkWqGabb6Yn8vKpYqhmH3IrqZremVsrGLJvnKp091lmm1miqt%2BYWx5iWqpc31%2BcV2Hu24%3D&ag=e&sag=28&codigo=0",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors", 
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"'
    }

    # Coleta todos os dados
    all_data = []
    start = 0
    length = 5000  # Tamanho conservador para evitar timeout
    draw = 1
    max_retries = 3

    # Cria sessão robusta com retry
    session = c.create_session_with_retries()

    print('Fazendo primeira requisição para descobrir total de registros...')

    # Payload base para as requisições
    def create_payload(draw, start, length):
        return {
            "draw": str(draw),
            "columns[0][data]": "0",
            "columns[0][name]": "codigo",
            "columns[0][searchable]": "true",
            "columns[0][orderable]": "true",
            "columns[0][search][value]": "",
            "columns[0][search][regex]": "false",
            "columns[1][data]": "1",
            "columns[1][name]": "nome",
            "columns[1][searchable]": "true",
            "columns[1][orderable]": "true",
            "columns[1][search][value]": "",
            "columns[1][search][regex]": "false",
            "columns[2][data]": "2",
            "columns[2][name]": "sigla",
            "columns[2][searchable]": "true",
            "columns[2][orderable]": "true",
            "columns[2][search][value]": "",
            "columns[2][search][regex]": "false",
            "columns[3][data]": "3",
            "columns[3][name]": "mes_ano_formatado",
            "columns[3][searchable]": "true",
            "columns[3][orderable]": "true",
            "columns[3][search][value]": "",
            "columns[3][search][regex]": "false",
            "columns[4][data]": "4",
            "columns[4][name]": "(case when t.mes_ano<='2021-10-01' then t.bf_qtd_fam else null",
            "columns[4][searchable]": "true",
            "columns[4][orderable]": "true",
            "columns[4][search][value]": "",
            "columns[4][search][regex]": "false",
            "columns[5][data]": "5",
            "columns[5][name]": "(case when t.mes_ano>='2023-03-01' then t.bf_qtd_fam else null",
            "columns[5][searchable]": "true",
            "columns[5][orderable]": "true",
            "columns[5][search][value]": "",
            "columns[5][search][regex]": "false",
            "columns[6][data]": "6",
            "columns[6][name]": "(case when t.mes_ano<='2021-10-01' then t.bf_vl else null end)",
            "columns[6][searchable]": "true",
            "columns[6][orderable]": "true",
            "columns[6][search][value]": "",
            "columns[6][search][regex]": "false",
            "columns[7][data]": "7",
            "columns[7][name]": "(case when t.mes_ano>='2023-03-01' then t.bf_vl else null end)",
            "columns[7][searchable]": "true",
            "columns[7][orderable]": "true",
            "columns[7][search][value]": "",
            "columns[7][search][regex]": "false",
            "columns[8][data]": "8",
            "columns[8][name]": "(case when t.mes_ano<='2021-10-01' then (case when t.bf_qtd_fam",
            "columns[8][searchable]": "true",
            "columns[8][orderable]": "true",
            "columns[8][search][value]": "",
            "columns[8][search][regex]": "false",
            "columns[9][data]": "9",
            "columns[9][name]": "(case when t.mes_ano>='2023-03-01' then (case when t.v2643>0 th",
            "columns[9][searchable]": "true",
            "columns[9][orderable]": "true",
            "columns[9][search][value]": "",
            "columns[9][search][regex]": "false",
            "order[0][column]": "2",
            "order[0][dir]": "asc",
            "order[1][column]": "0",
            "order[1][dir]": "asc",
            "start": str(start),
            "length": str(length),
            "search[value]": "",
            "search[regex]": "false"
        }

    payload = create_payload(draw, start, length)

    # Primeira requisição com retry
    retry_count = 0
    while retry_count < max_retries:
        try:
            response = session.post(url, headers=headers, data=payload, timeout=30)
            response.raise_for_status()
            first_page = response.json()
            break
        except Exception as e:
            retry_count += 1
            print(f"Erro na primeira requisição (tentativa {retry_count}/{max_retries}): {e}")
            if retry_count >= max_retries:
                raise
            print("Aguardando 5 segundos antes de tentar novamente...")
            sleep(5)

    records_total = first_page["recordsTotal"]
    print(f"Total de registros encontrados: {records_total}")

    all_data.extend(first_page["data"])

    # Continua coletando até obter todos os registros
    while len(all_data) < records_total:
        start += length
        draw += 1
        print(f"Coletando página com start={start}... ({len(all_data)}/{records_total} coletados)")
        
        payload = create_payload(draw, start, length)
        
        # Requisição com retry
        retry_count = 0
        while retry_count < max_retries:
            try:
                response = session.post(url, headers=headers, data=payload, timeout=30)
                response.raise_for_status()
                page = response.json()
                break
            except Exception as e:
                retry_count += 1
                print(f"Erro na requisição (tentativa {retry_count}/{max_retries}): {e}")
                if retry_count >= max_retries:
                    print(f"Falha após {max_retries} tentativas. Dados coletados até agora: {len(all_data)}")
                    raise
                print("Aguardando 5 segundos antes de tentar novamente...")
                sleep(5)
        
        all_data.extend(page["data"])
        
        # Pausa entre requisições para não sobrecarregar o servidor
        sleep(1)

    print(f"Total coletado: {len(all_data)} registros")

    # ************************
    # PROCESSAMENTO DOS DADOS
    # ************************

    print('Processando e limpando os dados...')

    # Converte lista de listas em DataFrame
    data = pd.DataFrame(all_data)

    # Define os nomes das colunas baseado na estrutura do payload
    column_names = [
        'Código',
        'Unidade Territorial',
        'UF',
        'Referência',
        'Famílias PBF (até Out/2021)',
        'Famílias PBF (a partir de Mar/2023)',
        'Valor repassado às famílias PBF (até Out/2021)',
        'Valor repassado às famílias PBF (a partir de Mar/2023)',
        'Valor do Benefício médio (até Out/2021)',
        'Valor do Benefício médio (a partir de Mar/2023)'
    ]
    data.columns = column_names

    # Converte tipos de dados
    data['Código'] = data['Código'].astype(str)
    data['Unidade Territorial'] = data['Unidade Territorial'].astype(str)
    data['UF'] = data['UF'].astype(str)
    data['Referência'] = data['Referência'].astype(str)
    data['Famílias PBF (até Out/2021)'] = pd.to_numeric(data['Famílias PBF (até Out/2021)'].astype(str).str.replace('.', '').str.replace(',', '.').str.replace('-', '').str.replace('R$', '').str.strip(), errors='coerce')
    data['Famílias PBF (a partir de Mar/2023)'] = pd.to_numeric(data['Famílias PBF (a partir de Mar/2023)'].astype(str).str.replace('.', '').str.replace(',', '.').str.replace('-', '').str.replace('R$', '').str.strip(), errors='coerce')
    data['Valor repassado às famílias PBF (até Out/2021)'] = pd.to_numeric(data['Valor repassado às famílias PBF (até Out/2021)'].astype(str).str.replace('.', '').str.replace(',', '.').str.replace('-', '').str.replace('R$', '').str.strip(), errors='coerce')
    data['Valor repassado às famílias PBF (a partir de Mar/2023)'] = pd.to_numeric(data['Valor repassado às famílias PBF (a partir de Mar/2023)'].astype(str).str.replace('.', '').str.replace(',', '.').str.replace('-', '').str.replace('R$', '').str.strip(), errors='coerce')
    data['Valor do Benefício médio (até Out/2021)'] = pd.to_numeric(data['Valor do Benefício médio (até Out/2021)'].astype(str).str.replace('.', '').str.replace(',', '.').str.replace('-', '').str.replace('R$', '').str.strip(), errors='coerce')
    data['Valor do Benefício médio (a partir de Mar/2023)'] = pd.to_numeric(data['Valor do Benefício médio (a partir de Mar/2023)'].astype(str).str.replace('.', '').str.replace(',', '.').str.replace('-', '').str.replace('R$', '').str.strip(), errors='coerce')

    print(f'Dados processados: {data.shape[0]} linhas, {data.shape[1]} colunas')

    # Salva o arquivo
    print(f'Salvando dados em: {file_path}')
    data.to_parquet(file_path, engine='pyarrow', compression='snappy', index=False)
    print(f'Arquivo {file_name} salvo com sucesso!')

    print('Download e processamento concluído!')

except:
    error = traceback.format_exc()
    with open(os.path.join(error_path, 'log_raw_programa_bolsa_familia.txt'), 'w', encoding='utf-8') as f:
        f.write(f'Erro em raw_programa_bolsa_familia.py em {pd.Timestamp.now().strftime("%d/%m/%Y %H:%M:%S")}\n')
        f.write(error)
    print('Erro ao baixar ou processar os dados. Verifique o log em "Doc/Municipios/log_raw_programa_bolsa_familia.txt".')