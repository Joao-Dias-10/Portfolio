import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# Defina as configurações do banco de dados
db_params = {
    "host": "172.31.50.236",           # Ex: 'localhost' ou o IP do servidor
    "database": "pugmil",      # Ex: 'meu_banco'
    "user": "postgres",        # Ex: 'postgres'
    "password": "Devmis*1"       # Ex: 'minhasenha'
}


# Caminho do arquivo CSV
csv_file = 'C:/Users/jpdias/SPEECH_NET_ALTO_VALOR_BASE_GERAL_DAS_PESQUISAS.csv'

# Lê o arquivo CSV com pandas
df = pd.read_csv(csv_file)

# Convertendo colunas 'unsigned' para 'signed' (int64, float64)
for col in df.select_dtypes(include=['uint64']).columns:
    df[col] = df[col].astype('int64')

# Cria a conexão com o banco de dados PostgreSQL usando sqlalchemy
engine = create_engine(f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}/{db_params['database']}")

# Usa o método to_sql do pandas para enviar os dados para o PostgreSQL
df.to_sql('SPEECH_NET_ALTO_VALOR_BASE_GERAL_DAS_PESQUISAS', engine, schema='public', if_exists='replace', index=False)

print("CSV carregado com sucesso no PostgreSQL!")