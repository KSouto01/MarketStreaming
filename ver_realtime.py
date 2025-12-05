import duckdb
import os
import pandas as pd
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_REALTIME = os.path.join(BASE_DIR, 'cma_realtime.duckdb')

print(f"🕵️ Auditando Banco: {DB_REALTIME}")

while True:
    try:
        if not os.path.exists(DB_REALTIME):
            print("❌ Arquivo .duckdb não existe ainda.")
        else:
            # Tenta conectar em modo leitura
            conn = duckdb.connect(DB_REALTIME, read_only=True)
            
            # Conta linhas
            count = conn.execute("SELECT count(*) FROM market_snapshot").fetchone()[0]
            
            if count > 0:
                print(f"✅ O Banco tem {count} linhas.")
                
                # Mostra amostra da CBOT
                df = conn.execute("SELECT symbol, Last, Time FROM market_snapshot WHERE group_name = 'CBOT' LIMIT 3").fetchdf()
                if not df.empty:
                    print("   Amostra CBOT:")
                    print(df)
                else:
                    print("   ⚠️ Banco tem dados, mas CBOT está vazia.")
            else:
                print("⚠️ Banco existe mas está VAZIO (0 linhas).")
            
            conn.close()
    except Exception as e:
        print(f"❌ Erro ao ler (Provável Lock): {e}")
    
    print("-" * 30)
    time.sleep(3)