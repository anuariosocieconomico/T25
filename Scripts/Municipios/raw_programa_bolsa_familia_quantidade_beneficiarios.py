import traceback
import functions as c
import os
import pandas as pd
from time import sleep


# obtém o caminho desse arquivo de comandos para armazenar diretamente no diretório raw
raw_path = c.raw_path
error_path = c.error_path
os.makedirs(raw_path, exist_ok=True)
os.makedirs(error_path, exist_ok=True)
db_name = 'programa_bolsa_familia_quantidade_beneficiarios'  # nome base da base de dados
file_name = 'raw_' + db_name + '.parquet'
file_path = os.path.join(raw_path, file_name)


# ************************
# DOWNLOAD DA BASE DE DADOS
# ************************

try:
    print('Iniciando download dos dados de quantidade de beneficiários do programa bolsa família...')

    # URL e headers para a API
    url = "https://aplicacoes.cidadania.gov.br/vis/data3/v.php?q[]=oNOhlMHqwGZsemeZ6au8srNeiJivm7mj2ejJZmx6aGepbHp%2BaGiJkWWeamOUqH1luL5sc610s6%2BjoLycmcuppsK2iJqdtKiftHTAw6RQzZhsn3iZvufAmXeulqbsnoiJnY7D1JileKbS6HCqcoFndd%2BaucGcaL3Cn92ibpjhrqCvrXB17K66ca1kkJhu0J6f0OCImp20qJ%2B0dLOvo6C8nG7dsqCA8YBtb4Obm%2BWssomdjsPUmKV4mb7nwJl3g6iv5lzDhXBlkseU1rCYmOGuoK%2BtcHXfmrnBnGiS1KjXYKmUtIZvoqmhrd50s6%2BjoLycbtCen9DgiG%2BvvaJd9FqwvZiZvNSWz2Vz07KGa2h4XmXcqK66nKC6xluqs2qWs3lkZXOYqdqlssGakn%2BhqaF2bImrdl%2Bft5am3qyws19tzZRsnWljhpzKb6Kpoa3edLOvo6C8nG7Qnp%2FQ4Ihvr72iXfRasL2YmbzUls9lc9OyhmxoeF5l3KiuupygusZbqrNqlrR5ZGVpsnXfmrnBnGi9wp%2Fdom6Y4a6gr61wdeyuunGtZYeRbtCen9DgiJqdtKiftHSzr6OgvJxu3bKggPF%2BZW57cKDapcCzcpO4zabPeG7D3LmnoYNwre6myZ%2BsjsXVnM6el8KbsZlcipqo3p8Q%2B5qWxtRTzqJTr%2BC7mJ1omZ%2BZfLaymJG4z5zLXVufzZBdX5mqm%2BettrKYkbyBl89ddcLpspr%2F9Zij6Kxtkaaax82Y16Kh0dy%2Fma9oXXy8iHZxiKK4z6fToZTB4G2YoWh3n%2BeesxHkkMDQpoqNpcbosp2uqVWD558Q8KWQwMJTkn%2BDpqRthH6OWIvumrvCoJG4xZiKoZh9vbKioa7459yivMFXg7jTnC3eqcLkwFSCqaKj5aKuwJygd6iY3bGUy%2B%2ByVGSKi4GiXJ7DmJvLypfLoZh937JUfq2jn9%2F8%2BrGgnMqBicuvnCAcw5mlu1WA2qa2uqCOycamiouo0e22rlxwd5DHYnCfrI7F1ZzOnpfCm7GZXIqaqN6fEPualsbUU8CepcY%2B7qqhsahav5q6t6OWuNOY3V12z%2BSuov%2Fvllqhe6N3Wn7MwqHeppe%2B37JUoK1VfN6nsrT62rrKot1dib7ttvfdvpqj7FmTr6SWw8qU3KKmfbyxo6itqJ3ep8GzV1WZt3STYITS3LuopayWnt5ZsbNXb7zPmNAA4MDkvKdcnpas4vzuxJyWyoF5y6qcyeSupqG7WIvumrvCoJG4xZiKoZh9vbKioa7459yivMFXg7jTnC3eqcLkwFSCqaKj5aKuwJygd6Sl056hICKuVGSKi2OZnm2Pm5zDxqbNoqHR4G1cfp52Y5msvLuYkcbUVruylMvvtpidrJpa3Z5tkJybvMf2F6CczO5tebS8p5voq7G3pfD405zZsFPB4G2IrqmjreL89BHanHeJda%2BRXIDMwpWqvJ6e2p2ybpuSd6eU1wDgyeSup1yrpKeZrMLBp5LF1PYNrFPN3L%2BXpamhWt2obbCcm7zH9hegnMybvZmot1WN3qDCwKZNm8aZz7Ci2euIsLjEZ2qrbHp%2BalqHkoeabW2Nq4dkbKJwbKlrgXtnZISRZL5tY5erfW5seI8%3D&ag=m&wt=json&tp_funcao_consulta=0&rqprocess=180dafcea2bc3fa62ef89f9983e033f4"

    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "DNT": "1",
        "Origin": "https://aplicacoes.cidadania.gov.br",
        "Referer": "https://aplicacoes.cidadania.gov.br/vis/data3/v.php",
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

    # Payload base para as requisições (baseado no curl fornecido)
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
            "columns[4][name]": "v794",
            "columns[4][searchable]": "true",
            "columns[4][orderable]": "true",
            "columns[4][search][value]": "",
            "columns[4][search][regex]": "false",
            "columns[5][data]": "5",
            "columns[5][name]": "v795",
            "columns[5][searchable]": "true",
            "columns[5][orderable]": "true",
            "columns[5][search][value]": "",
            "columns[5][search][regex]": "false",
            "columns[6][data]": "6",
            "columns[6][name]": "v692",
            "columns[6][searchable]": "true",
            "columns[6][orderable]": "true",
            "columns[6][search][value]": "",
            "columns[6][search][regex]": "false",
            "columns[7][data]": "7",
            "columns[7][name]": "v797",
            "columns[7][searchable]": "true",
            "columns[7][orderable]": "true",
            "columns[7][search][value]": "",
            "columns[7][search][regex]": "false",
            "columns[8][data]": "8",
            "columns[8][name]": "v393",
            "columns[8][searchable]": "true",
            "columns[8][orderable]": "true",
            "columns[8][search][value]": "",
            "columns[8][search][regex]": "false",
            "columns[9][data]": "9",
            "columns[9][name]": "v798",
            "columns[9][searchable]": "true",
            "columns[9][orderable]": "true",
            "columns[9][search][value]": "",
            "columns[9][search][regex]": "false",
            "columns[10][data]": "10",
            "columns[10][name]": "v799",
            "columns[10][searchable]": "true",
            "columns[10][orderable]": "true",
            "columns[10][search][value]": "",
            "columns[10][search][regex]": "false",
            "columns[11][data]": "11",
            "columns[11][name]": "(case when coalesce(t.v798%2C0)%2Bcoalesce(t.v799%2C0)+%3E+0+then+coale",
            "columns[11][searchable]": "true",
            "columns[11][orderable]": "true",
            "columns[11][search][value]": "",
            "columns[11][search][regex]": "false",
            "columns[12][data]": "12",
            "columns[12][name]": "(case when coalesce(t.v797%2C0)%2Bcoalesce(t.v798%2C0)%2Bcoalesce(t.v79",
            "columns[12][searchable]": "true",
            "columns[12][orderable]": "true",
            "columns[12][search][value]": "",
            "columns[12][search][regex]": "false",
            "columns[13][data]": "13",
            "columns[13][name]": "v800",
            "columns[13][searchable]": "true",
            "columns[13][orderable]": "true",
            "columns[13][search][value]": "",
            "columns[13][search][regex]": "false",
            "columns[14][data]": "14",
            "columns[14][name]": "v1123",
            "columns[14][searchable]": "true",
            "columns[14][orderable]": "true",
            "columns[14][search][value]": "",
            "columns[14][search][regex]": "false",
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
        'Mês/Ano',
        'Quantidade de Beneficiários de Renda de Cidadania - BRC',
        'Quantidade de Benefícios Complementares - BRC',
        'Quantidade de Benefícios Primeira Infância - BPI',
        'Quantidade de Benefícios Variáveis Familiares Gestantes - BVG',
        'Quantidade de Benefícios Variáveis Familiares Nutriz - BVN',
        'Quantidade de Benefícios Variáveis Familiares Criança - BV',
        'Quantidade de Benefícios Variáveis Familiares Adolescente - BVA',
        'Quantidade de Benefícios Variáveis Familiares Criança e Adolescente Somados',
        'Quantidade de Benefícios Variáveis Familiares',
        'Quantidade de Benefícios Extraordinários de Transição - BET',
        'Quantidade de Famílias com suspensão parcial do benefício pelo Seguro Defeso'
    ]
    data.columns = column_names

    # Converte tipos de dados
    data['Código'] = data['Código'].astype(str)
    data['Unidade Territorial'] = data['Unidade Territorial'].astype('category')
    data['UF'] = data['UF'].astype('category')
    data['Mês/Ano'] = data['Mês/Ano'].astype(str)

    # Colunas numéricas - remove pontos (separador de milhares) e converte para numérico
    numeric_columns = [
        'Quantidade de Beneficiários de Renda de Cidadania - BRC',
        'Quantidade de Benefícios Complementares - BRC',
        'Quantidade de Benefícios Primeira Infância - BPI',
        'Quantidade de Benefícios Variáveis Familiares Gestantes - BVG',
        'Quantidade de Benefícios Variáveis Familiares Nutriz - BVN',
        'Quantidade de Benefícios Variáveis Familiares Criança - BV',
        'Quantidade de Benefícios Variáveis Familiares Adolescente - BVA',
        'Quantidade de Benefícios Variáveis Familiares Criança e Adolescente Somados',
        'Quantidade de Benefícios Variáveis Familiares',
        'Quantidade de Benefícios Extraordinários de Transição - BET',
        'Quantidade de Famílias com suspensão parcial do benefício pelo Seguro Defeso'
    ]

    for col in numeric_columns:
        data[col] = pd.to_numeric(
            data[col].astype(str).str.replace('.', '').str.replace(',', '.').str.replace('-', ''), 
            errors='coerce'
        ).astype('Int64')

    print(f'Dados processados: {data.shape[0]} linhas, {data.shape[1]} colunas')

    # Salva o arquivo
    print(f'Salvando dados em: {file_path}')
    data.to_parquet(file_path, engine='pyarrow', compression='snappy', index=False)
    print(f'Arquivo {file_name} salvo com sucesso!')

    print('Download e processamento concluído!')

except:
    error = traceback.format_exc()
    with open(os.path.join(error_path, 'log_raw_programa_bolsa_familia_quantidade_beneficiarios.txt'), 'w', encoding='utf-8') as f:
        f.write(f'Erro em raw_programa_bolsa_familia_quantidade_beneficiarios.py em {pd.Timestamp.now().strftime("%d/%m/%Y %H:%M:%S")}\n')
        f.write(error)
    print('Erro ao baixar ou processar os dados. Verifique o log em "Doc/Municipios/log_raw_programa_bolsa_familia_quantidade_beneficiarios.txt".')