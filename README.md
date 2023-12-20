# Desafio de Data Engineer Jr.

## Descrição
O objetivo deste desafio é capturar, estruturar, armazenar e transformar dados de uma API instantânea. A API consiste nos dados de GPS do BRT que são gerados na hora da consulta com o último sinal transmitido por cada veículo.

Para o desafio, será necessário construir uma pipeline que captura os dados minuto a minuto e gera um arquivo no formato CSV. O arquivo gerado deverá conter no mínimo 10 minutos de dados capturados (estruture os dados da maneira que achar mais conveniente), então carregue os dados para uma tabela no Postgres. Por fim, crie uma tabela derivada usando o DBT. A tabela derivada deverá conter o ID do onibus, posição e sua a velocidade.

A pipeline deverá ser construída subindo uma instância local do Prefect (em Python). Utilize a versão *0.15.9* do Prefect.

## Execução da pipeline
1. Clonar o repositório do projeto: `git clone https://github.com/gabryellesoares/emd-desafio-data-eng`
2. Navegar até o diretório: `cd emd-desafio-data-eng/`
3. Criar um ambiente Python utilizando `conda` ou `venv`
4. Instalar as dependências com `pip install -r requirements.txt`
5. Crie um arquivo `.env` com as informações do seu banco de dados local com a seguinte estrutura: 
```
POSTGRES_HOST = 'host'
POSTGRES_PORT = port
POSTGRES_DB = 'database'
POSTGRES_USER = 'user'
POSTGRES_PASSWORD = 'password'
```
6. Executar a pipeline usando `python run.py`