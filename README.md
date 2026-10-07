# Marketplace Data Engineering Pipeline

Projeto de Engenharia de Dados desenvolvido para construir um pipeline completo de ingestão, transformação, armazenamento e orquestração de dados de produtos de um marketplace.

O projeto utiliza uma API pública como fonte de dados e processa as informações seguindo uma arquitetura em camadas Bronze, Silver e Gold.

## Arquitetura do Projeto

Fluxo do pipeline:

DummyJSON API  
↓  
Bronze - JSON  
↓  
Silver - Parquet  
↓  
Gold - Parquet  
↓  
PostgreSQL  
↓  
Apache Airflow

## Tecnologias Utilizadas

- Python
- Apache Spark / PySpark
- Apache Airflow
- PostgreSQL
- Docker
- Docker Compose
- Apache Parquet
- Git

## Fonte de Dados

Os dados são obtidos através da API pública DummyJSON.

Endpoint utilizado:

```text
https://dummyjson.com/products?limit=0