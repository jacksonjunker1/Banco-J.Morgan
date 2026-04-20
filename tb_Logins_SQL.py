import pandas as pd
import pyodbc as sql
import os
from gerar_conexao_sql import gerar_conexao

caminho_logins = r"C:\Users\jacks\OneDrive\Documentos\Banco J.Morgan\logins.csv"

try:
    if os.path.exists(caminho_logins):
        arquivo_logins = pd.read_csv(caminho_logins)
        print("Dados Carregados Com Sucesso...")
except Exception as e:
    print(f"Falha ao Carregar dados...{e}")

if __name__ == "__main__":
    conexao = gerar_conexao()
    dados_logins = []

    for linha in arquivo_logins.itertuples(index=False):
        dados_logins.append((
            linha.ID_Cliente,
            linha.Usuario,
            linha.Senha_Hash,
            linha.Nivel_Acesso,
            linha.Ultimo_Acesso
        ))

def tabela_logins():
    cursor = conexao.cursor()

    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Tb_Logins')
        CREATE TABLE Tb_Logins (
            ID_Login int identity(1,1) primary key,
            ID_Cliente int
                references Tb_Clientes(ID_Cliente) not null,
            Usuario char(50) not null,
            Senha_Hash char(255) not null,
            Nivel_Acesso char(50) not null,
            Ultimo_Acesso varchar(100) not null
        )

        ''')
    
    query = 'INSERT INTO Tb_Logins(ID_Cliente,Usuario,Senha_Hash,Nivel_Acesso,Ultimo_Acesso) VALUES(?, ?, ?, ?, ?)'
    cursor.executemany(query,dados_logins)

if __name__ == "__main__":
    try:
        tabela_logins()
        conexao.commit()
        print(f'Tabela Criada e {len(dados_logins)} Dados Inseridos Com Sucesso')
    except Exception as e:
        conexao.rollback()
        print(f'Falha ao Carregar Dados{e}')
    finally:
        conexao.close()
        print('Conexao encerrada...')
        