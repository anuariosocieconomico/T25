# Usar a imagem oficial do Python
FROM python:3.10

# Definir o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copiar o conteúdo do repositório para dentro do contêiner
COPY Scripts/Daniel/requirements.txt /app/

# Instalar as dependências do projeto
RUN pip install --no-cache-dir -r requirements.txt

RUN apt-get update && apt-get install -y wget unzip && \
    wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    apt-get install -y ./google-chrome-stable_current_amd64.deb && \
    rm google-chrome-stable_current_amd64.deb && \
    apt-get clean

COPY . /app

# Comando para executar o script
CMD ["python", "Scripts/Daniel/run_scripts.py"]
