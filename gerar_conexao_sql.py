import pyodbc as sql

def gerar_conexao():
    conexao = sql.connect(
        'DRIVER={SQL Server};'
        'SERVER=JACKSON;'
        'DATABASE=Banco J.Morgan;'
        'Trusted_Connection=yes;'
    )
    return conexao