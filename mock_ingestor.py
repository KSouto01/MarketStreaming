import duckdb
import time
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# --- CONFIGURAÇÃO DOS PRODUTOS FICTÍCIOS ---
PRODUCTS = {
    "B3": [
        {"name": "Dolar", "root": "DOL", "base_price": 5.80, "volatility": 0.05},
        {"name": "Milho", "root": "CCM", "base_price": 65.50, "volatility": 0.80},
        {"name": "Boi", "root": "BGI", "base_price": 245.00, "volatility": 2.50}
    ],
    "CBOT": [
        {"name": "Soja", "root": "ZS", "base_price": 1250.00, "volatility": 8.00},
        {"name": "Milho", "root": "ZC", "base_price": 480.00, "volatility": 5.00},
        {"name": "Farelo", "root": "ZM", "base_price": 350.00, "volatility": 4.00},
        {"name": "Oleo", "root": "ZL", "base_price": 55.00, "volatility": 1.50}
    ]
}

MONTH_CODES = {1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M', 7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'}
DB_FILE = 'mock_data.duckdb'

def init_db():
    try:
        with duckdb.connect(DB_FILE) as conn:
            conn.execute("DROP TABLE IF EXISTS market_snapshot")
            conn.execute("DROP TABLE IF EXISTS market_history")
            conn.execute("""
                CREATE TABLE market_snapshot (
                    timestamp TIMESTAMP, group_name VARCHAR, product_name VARCHAR, symbol VARCHAR, description VARCHAR,
                    Last DOUBLE, High DOUBLE, Low DOUBLE, Open DOUBLE, Change DOUBLE, PChange DOUBLE, Previous DOUBLE, 
                    Volume DOUBLE, Time VARCHAR, Bid DOUBLE, Ask DOUBLE, Maturity DATE
                )
            """)
            conn.execute("""
                CREATE TABLE market_history (
                    symbol VARCHAR, date_ref DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE
                )
            """)
        print("✅ Banco de Testes Criado (mock_data.duckdb)")
    except Exception as e: print(f"❌ Erro ao criar DB: {e}")

def generate_history():
    """Gera histórico com arredondamento estrito de 2 casas decimais"""
    rows = []
    price = 5.50
    today = datetime.now()
    
    # Simula 90 dias
    for i in range(90, 0, -1):
        date = today - timedelta(days=i)
        
        # Variação maior para garantir candles de baixa visíveis
        change = random.uniform(-0.10, 0.10) 
        
        open_p = round(price, 2)
        close_p = round(price + change, 2)
        
        # Garante que High e Low sejam consistentes com Open e Close
        # Adiciona um 'spread' para garantir que o candle tenha pavio
        high_p = round(max(open_p, close_p) + random.uniform(0.02, 0.10), 2)
        low_p = round(min(open_p, close_p) - random.uniform(0.02, 0.10), 2)
        
        vol = random.randint(5000, 50000)
        price = close_p # O fechamento de hoje é a base para amanhã
        
        symbol = f"DOL{MONTH_CODES[today.month]}25" 
        rows.append((symbol, date.date(), open_p, high_p, low_p, close_p, vol))
    
    with duckdb.connect(DB_FILE) as conn:
        conn.executemany("INSERT INTO market_history VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
    print("📊 Histórico Fictício Gerado (2 casas decimais).")

def generate_snapshot():
    rows = []
    now = datetime.now()
    time_str = now.strftime("%H:%M:%S")
    
    for group, items in PRODUCTS.items():
        for prod in items:
            base = prod['base_price']
            for i in range(6):
                future_date = now + relativedelta(months=i)
                m_char = MONTH_CODES[future_date.month]
                symbol = f"{prod['root']}{m_char}{future_date.strftime('%y')}"
                
                variation = random.uniform(-prod['volatility'], prod['volatility'])
                
                # Arredondamento em todas as pontas
                last = round(base + (i * (base * 0.01)) + variation, 2)
                open_p = round(last - variation, 2)
                high = round(last + abs(variation/2), 2)
                low = round(last - abs(variation/2), 2)
                bid = round(last - 0.01, 2)
                ask = round(last + 0.01, 2)
                prev = round(open_p, 2)
                
                change = round(last - prev, 2)
                pchange = round((change / prev) * 100, 2) if prev else 0
                vol = random.randint(100, 5000)

                rows.append((
                    now, group, prod['name'], symbol, f"{prod['name']} Simulado",
                    last, high, low, open_p, change, pchange, prev,
                    vol, time_str, bid, ask, future_date.replace(day=1).date()
                ))
    
    with duckdb.connect(DB_FILE) as conn:
        conn.execute("DELETE FROM market_snapshot")
        conn.executemany("INSERT INTO market_snapshot VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    
    print(f"[{time_str}] 🎲 Mocks Atualizados.")

if __name__ == "__main__":
    print("🚀 Iniciando Gerador de Mocks (Arredondamento Ajustado)...")
    init_db()
    generate_history()
    
    while True:
        try:
            generate_snapshot()
            time.sleep(2)
        except KeyboardInterrupt:
            print("🛑 Parando.")
            break