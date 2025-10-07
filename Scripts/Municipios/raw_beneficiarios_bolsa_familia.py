import functions as c
import os
import pandas as pd
from time import sleep


# obtém o caminho desse arquivo de comandos para armazenar diretamente no diretório raw
raw_path = c.raw_path
db_name = 'beneficiarios_bolsa_familia'  # nome base da base de dados
file_name = 'raw_' + db_name + '.parquet'
file_path = os.path.join(raw_path, file_name)


# ************************
# DOWNLOAD DA BASE DE DADOS
# ************************

print('Iniciando download dos dados de benefícios sociais...')

# URL e headers para a API
url = "https://aplicacoes.cidadania.gov.br/vis/data3/v.php?q[]=r6JtZJCuhLBtxKW25rV%2FfmhghJJll21kmK19ZnB1ZXGmaX7Ksk66wqbPXarF4LtUfLWardiau71zan6TY5xuYI6remRtb1Wb551tjqSSysCU2KxxmqJ%2FZG17YmurZn1%2FXk3LyZjYXXPTrYNmdWiapuyebbysmcOBmNihVNq2s5Wou5p135q5wZxokseU1rCYmLbAqalrsFvcmsCzV6S%2FxqGKfaDC7qyVqrdzd6BrfYBqWoeUYJpuWn3vtZmqaHWwq29%2Fh1eSw9SYiquoyedtmaqsVre0n666qpKSx5TWsJiYtrOVqLuadbSswruzfbzUptmepn3dsqKhrp6d4vzuwKCOyoGh2V2Dn8FtXJ28%2BOOZiMLCZl%2BHk2STYIPC7sCjnbtVnN6nsrSgkMAk1NymlNCbu6NcmHeAmWGubqeOydWc3F2Xwpuala53Z2qrbHbKp2jT3a%2BcbWSQqH5maXhmjqlph35nZ4eRjaU%3D&ag=m&wt=json&tp_funcao_consulta=0&rqprocess=b633eb92dc9efde66db2699f8177acf6"

headers = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://aplicacoes.cidadania.gov.br",
    "Referer": "https://aplicacoes.cidadania.gov.br/vis/data3/v.php",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest",
}

# Coleta todos os dados
all_data = []
start = 0
length = 50000  # Reduzido para evitar timeout
draw = 1
max_retries = 3

# Cria sessão robusta com retry
session = c.create_session_with_retries()

print('Fazendo primeira requisição para descobrir total de registros...')
payload = {
    "draw": str(draw),
    "start": str(start),
    "length": str(length),
    "search[value]": "",
    "search[regex]": "false",
    "order[0][column]": "2",
    "order[0][dir]": "asc",
    "order[1][column]": "0",
    "order[1][dir]": "asc"
}

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
        import time
        time.sleep(5)

records_total = first_page["recordsTotal"]
print(f"Total de registros encontrados: {records_total}")

all_data.extend(first_page["data"])

# Continua coletando até obter todos os registros
while len(all_data) < records_total:
    start += length
    draw += 1
    print(f"Coletando página com start={start}... ({len(all_data)}/{records_total} coletados)")
    
    payload.update({
        "draw": str(draw),
        "start": str(start)
    })
    
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
            import time
            time.sleep(5)
    
    all_data.extend(page["data"])
    
    # Pausa entre requisições para não sobrecarregar o servidor
    import time
    time.sleep(1)

print(f"Total coletado: {len(all_data)} registros")

# ************************
# PROCESSAMENTO DOS DADOS
# ************************

print('Processando e limpando os dados...')

# Converte lista de listas em DataFrame
data = pd.DataFrame(all_data)

# Define os nomes das colunas
column_names = ['Código', 'Unidade Territorial', 'UF', 'Referência', 'Beneficiários até Out/21', 'Beneficiários a partir Mar/23']
data.columns = column_names

# Converte tipos de dados
data['Código'] = data['Código'].astype(str)
data['Unidade Territorial'] = data['Unidade Territorial'].astype('category')
data['UF'] = data['UF'].astype('category')
data['Referência'] = data['Referência'].astype(str)
data['Beneficiários até Out/21'] = pd.to_numeric(data['Beneficiários até Out/21'].str.replace('.', '').str.replace('-', ''))
data['Beneficiários a partir Mar/23'] = pd.to_numeric(data['Beneficiários a partir Mar/23'].str.replace('.', '').str.replace('-', ''))

print(f'Dados processados: {data.shape[0]} linhas, {data.shape[1]} colunas')

# Salva o arquivo
print(f'Salvando dados em: {file_path}')
data.to_parquet(file_path, engine='pyarrow', compression='snappy', index=False)
print(f'Arquivo {file_name} salvo com sucesso!')

print('Download e processamento concluído!')