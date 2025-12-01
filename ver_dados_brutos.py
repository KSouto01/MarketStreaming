import duckdb
import os
import pandas as pd

# Garante leitura no lugar certo
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(BASE_DIR, 'cma_data.duckdb')

print(f"📂 Auditando: {db_path}\n")

if os.path.exists(db_path):
    try:
        conn = duckdb.connect(db_path, read_only=True)
        
        # 1. Pega amostra da B3 (Dólar)
        print("--- AMOSTRA B3 (DÓLAR) ---")
        df_b3 = conn.execute("""
            SELECT symbol, Last, Change, PChange, Time 
            FROM market_snapshot 
            WHERE group_name = 'B3' AND product_name = 'Dolar'
            LIMIT 3
        """).fetchdf()
        print(df_b3 if not df_b3.empty else "⚠️ Nenhum dado de Dólar gravado.")
        
        # 2. Pega amostra da CBOT (Soja/Milho)
        print("\n--- AMOSTRA CBOT ---")
        df_cbot = conn.execute("""
            SELECT symbol, Last, Change, PChange, Time 
            FROM market_snapshot 
            WHERE group_name = 'CBOT'
            LIMIT 3
        """).fetchdf()
        print(df_cbot if not df_cbot.empty else "⚠️ Nenhum dado da CBOT gravado.")
        
    except Exception as e:
        print(f"❌ Erro ao ler banco: {e}")
else:
    print("❌ Arquivo de banco de dados não encontrado.")