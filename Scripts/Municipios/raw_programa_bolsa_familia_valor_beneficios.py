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
db_name = 'programa_bolsa_familia_valor_beneficios'  # nome base da base de dados
file_name = 'raw_' + db_name + '.parquet'
file_path = os.path.join(raw_path, file_name)


# ************************
# DOWNLOAD DA BASE DE DADOS
# ************************

try:
    print('Iniciando download dos dados de valor de benefícios do programa bolsa família...')

    # URL e headers para a API
    url = "https://aplicacoes.cidadania.gov.br/vis/data3/v.php?q[]=oNOhlMHqwGZsemeZ6au8srNeiJmvm7mj2ejJZmx6Zmeqa3p%2BaGiJkWWeamOUqH1luL5taqp0s6%2BjoLycmcuppsK2iJqdtKiftHTAw6RQzZljnHiZvufAmXeulqbsnoiJnY7D1JileKbS6HCqcoFodd%2BaucGcaL3Cn92ibpjhrqCvrXB17K66ca1lh5Vu0J6f0OCImp20qJ%2B0dLOvo6C8nG7dsqCA8X9lcIObm%2BWssomdjsPUmKV4mb7nwJl3g6iv5lzDhmdikseU1rCYmOGuoK%2BtcHXfmrnBnGiS1KjXYKmVq4Nvoqmhrd50s6%2BjoLycbtCen9DgiG%2BvvaJd9FqwvZiZvNSWz2Vz07N9aWh4XlqkWbC9mJm81JbPZXPTs31qaHheW%2FZ0s6%2BjoLycmcuppsK2iJqdtKiftHTAw6RQ0oKW2Z6fwu6wmWSIq3Kpb3l%2BYE2CgZbZnp%2FC7rCZZIirbKpteX5gTYKBltmen8LusJlkiKtyqW15fmBNgoGW2Z6fwu6wmWSIq3Kpbnl%2BYE7UnJnLqabCtrOVqLuadbSfrrqqkpKcpt%2BqVtOzfWt3rpam7J6ItJiZysZupaOUye6yb3e7qqecr35%2FaWGSx5TWsJiY4a6gr61wdd%2BaucGcaJLUqNe5ib7nvKZcnKSu2qVtoJyduNSmy6Gifd%2ByVH6to5%2Ff%2FPqxoJzKgZfPXYXC6bGVXKyaWryisa%2BbjsXKlI2TlMnqv1SQt6mb5Vmfs6eOytSUzqxTweBtdqG2mqA85rC3pqB3pKLXrZ%2FC6LKisKmnn%2BxZdZB6fICEicupos%2BboaOwqaFay569r6qguMWiiqGYfb2yoqGu%2BOfcorzBV33JyqDPpqW%2Bm5aiogvXqNyirm5fb6eqXI2TlMnqv1SQt6mb5Vmfs6eOytSUzqxTweBtdqG2mqA85rC3pqB3t5Tcpvb%2B8bKdr2h7m%2BaiubeYn7zUU7GiptHcu6ihaF18z4B2cY2Ow9ClipGi0dy5VI6tpZvsrK6ypk27xlOsoqHC4RDhn7GkrZmPrsCg8PjXmNOwU6Pcup2osZas3qxtnKyhycqtimV1s8l2V5KpoanrWaG9q47DgYXPrZTQ7q6Yq2iZn5l7sryckxoOltOspn3RrqalC9aw3qLAbn2OxMqf056lwu5td66xlqg84K5uX2%2BtilbAnp%2FM7W2Iq7yWppmLsr6YoMrCl9ldl8Kbj5mqrZv9Jpy2vapNrcKl0wDU0%2BC2p1yOlqfipbavqZLKgXTOrJ%2FC7rCZqryaWqF7o49gUK3Cn9mvU7HqwZWoaIef6ZrAwZiRxoGXz111wumymv%2F1mKPorG2kmJ%2FAJNTgopzQm5OVqbGho9qrssFXcMnKlNgA2r6bdXaScVWfmXqxvaOSysSY2LGYfaOPin1xVa3opq6ypqB6t5TWrKV9z7yonbRVjN6prsGqjrvQU86iU5%2Fgu5miC%2BKd4qjAbo2Oycr2C7OYxu5tep21nqbimr%2BzqlCtwp%2FZr1Ox6sGVqGiHn%2BmawMGYkcaBl89ddcLpspr%2F9Zij6Kxtk6%2BhycKi3KGcyz7upqW3qFrdnm2iqY7F1Jwt5PYA6m1cfo2JY5yPrrqmn3e1ot6en33NsqSdu6ib3ahts6RNmcahz6P2Ct62o69omKnmWcDDqp28z6Yt4KJ9666mn7GWppmpsrqmTarGmt%2Bvon2%2Fspqhu6S26XTJyrNfh5Nml21miqt%2BiGx4b2qpc31%2BkWiJkWWeamOUqH1lkHhldKlph35nhw%3D%3D&ag=m&wt=json&tp_funcao_consulta=0&rqprocess=7928940f8688528511a4d8528a374b00"

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
    length = 25000  # Tamanho conservador para evitar timeout
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
            "columns[4][name]": "v801",
            "columns[4][searchable]": "true",
            "columns[4][orderable]": "true",
            "columns[4][search][value]": "",
            "columns[4][search][regex]": "false",
            "columns[5][data]": "5",
            "columns[5][name]": "v802",
            "columns[5][searchable]": "true",
            "columns[5][orderable]": "true",
            "columns[5][search][value]": "",
            "columns[5][search][regex]": "false",
            "columns[6][data]": "6",
            "columns[6][name]": "v693",
            "columns[6][searchable]": "true",
            "columns[6][orderable]": "true",
            "columns[6][search][value]": "",
            "columns[6][search][regex]": "false",
            "columns[7][data]": "7",
            "columns[7][name]": "v804",
            "columns[7][searchable]": "true",
            "columns[7][orderable]": "true",
            "columns[7][search][value]": "",
            "columns[7][search][regex]": "false",
            "columns[8][data]": "8",
            "columns[8][name]": "v214",
            "columns[8][searchable]": "true",
            "columns[8][orderable]": "true",
            "columns[8][search][value]": "",
            "columns[8][search][regex]": "false",
            "columns[9][data]": "9",
            "columns[9][name]": "v805",
            "columns[9][searchable]": "true",
            "columns[9][orderable]": "true",
            "columns[9][search][value]": "",
            "columns[9][search][regex]": "false",
            "columns[10][data]": "10",
            "columns[10][name]": "v806",
            "columns[10][searchable]": "true",
            "columns[10][orderable]": "true",
            "columns[10][search][value]": "",
            "columns[10][search][regex]": "false",
            "columns[11][data]": "11",
            "columns[11][name]": "(case when coalesce(t.v805%2C0) + coalesce(t.v806%2C0) > 0 then coa",
            "columns[11][searchable]": "true",
            "columns[11][orderable]": "true",
            "columns[11][search][value]": "",
            "columns[11][search][regex]": "false",
            "columns[12][data]": "12",
            "columns[12][name]": "(case when coalesce(t.v806%2C0)%2Bcoalesce(t.v214%2C0)%2Bcoalesce(t.v80",
            "columns[12][searchable]": "true",
            "columns[12][orderable]": "true",
            "columns[12][search][value]": "",
            "columns[12][search][regex]": "false",
            "columns[13][data]": "13",
            "columns[13][name]": "v807",
            "columns[13][searchable]": "true",
            "columns[13][orderable]": "true",
            "columns[13][search][value]": "",
            "columns[13][search][regex]": "false",
            "columns[14][data]": "14",
            "columns[14][name]": "v1124",
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

    # Define os nomes das colunas conforme especificado
    column_names = [
        'Código',
        'Unidade Territorial',
        'UF',
        'Referência',
        'Valor Total Repassado de Benefícios de Renda de Cidadania',
        'Valor Total Repassado de Benefícios Complementares (BCO)',
        'Valor Total Repassado de Benefícios Primeira Infância (BPI)',
        'Valor Total Repassado de Benefícios Variáveis Familiares Gestante (BVG)',
        'Valor Total Repassado de Benefícios Variáveis Familiares Nutriz (BVN)',
        'Valor Total Repassado de Benefícios Variáveis Familiares Criança (BV)',
        'Valor Total Repassado de Benefícios Variáveis Familiares Adolescente (BVA)',
        'Valor Total Repassado de Benefícios Variáveis Familiares Criança (BV) e Adolescente (BVA) somados',
        'Valor Total Repassado de Benefícios Variáveis Familiares',
        'Valor Total Repassado de Benefícios Extraordinários de Transição (BET)',
        'Valor Total Repassado em Benefícios com suspensão parcial pelo Seguro Defeso'
    ]
    data.columns = column_names

    # Converte tipos de dados
    data['Código'] = data['Código'].astype(str)
    data['Unidade Territorial'] = data['Unidade Territorial'].astype('category')
    data['UF'] = data['UF'].astype('category')
    data['Referência'] = data['Referência'].astype(str)

    # Colunas de valores - remove pontos (separador de milhares), vírgulas (separador decimal) e converte para numérico
    value_columns = [
        'Valor Total Repassado de Benefícios de Renda de Cidadania',
        'Valor Total Repassado de Benefícios Complementares (BCO)',
        'Valor Total Repassado de Benefícios Primeira Infância (BPI)',
        'Valor Total Repassado de Benefícios Variáveis Familiares Gestante (BVG)',
        'Valor Total Repassado de Benefícios Variáveis Familiares Nutriz (BVN)',
        'Valor Total Repassado de Benefícios Variáveis Familiares Criança (BV)',
        'Valor Total Repassado de Benefícios Variáveis Familiares Adolescente (BVA)',
        'Valor Total Repassado de Benefícios Variáveis Familiares Criança (BV) e Adolescente (BVA) somados',
        'Valor Total Repassado de Benefícios Variáveis Familiares',
        'Valor Total Repassado de Benefícios Extraordinários de Transição (BET)',
        'Valor Total Repassado em Benefícios com suspensão parcial pelo Seguro Defeso'
    ]

    for col in value_columns:
        data[col] = pd.to_numeric(
            data[col].astype(str).str.replace('.', '').str.replace(',', '.').str.replace('-', '').str.replace('R$', '').str.strip(),
            errors='coerce'
        )

    print(f'Dados processados: {data.shape[0]} linhas, {data.shape[1]} colunas')

    # Salva o arquivo
    print(f'Salvando dados em: {file_path}')
    data.to_parquet(file_path, engine='pyarrow', compression='snappy', index=False)
    print(f'Arquivo {file_name} salvo com sucesso!')

    print('Download e processamento concluído!')

except:
    error = traceback.format_exc()
    with open(os.path.join(error_path, 'log_raw_programa_bolsa_familia_valor_beneficios.txt'), 'w', encoding='utf-8') as f:
        f.write(f'Erro em raw_programa_bolsa_familia_valor_beneficios.py em {pd.Timestamp.now().strftime("%d/%m/%Y %H:%M:%S")}\n')
        f.write(error)
    print('Erro ao baixar ou processar os dados. Verifique o log em "Doc/Municipios/log_raw_programa_bolsa_familia_valor_beneficios.txt".')