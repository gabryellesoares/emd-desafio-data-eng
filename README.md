# Desafio de Data Engineer - EMD

Repositório de instrução para o desafio técnico para vaga de Pessoa Engenheira de Dados no Escritório de Dados do Rio de Janeiro

## Descrição do desafio

Neste desafio você deverá capturar, estruturar, armazenar e transformar dados de uma API instantânea. A API consiste nos dados de GPS do BRT que são gerados na hora da consulta com o último sinal transmitido por cada veículo.

Para o desafio, será necessário construir uma pipeline que captura os dados minuto a minuto e gera um arquivo no formato CSV. O arquivo gerado deverá conter no mínimo 10 minutos de dados capturados (estruture os dados da maneira que achar mais conveniente), então carregue os dados para uma tabela no Postgres. Por fim, crie uma tabela derivada usando o DBT. A tabela derivada deverá conter o ID do onibus, posição e sua a velocidade.

A pipeline deverá ser construída subindo uma instância local do Prefect (em Python). Utilize a versão *0.15.9* do Prefect.

