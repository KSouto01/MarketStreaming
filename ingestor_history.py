import os
import json
import time
import requests
import duckdb
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))
DB_HISTORY = os.path.join(BASE_DIR, 'cma_history.duckdb')
CHART_FILE = os.path.join(BASE_DIR, 'active_chart.json')

# Códigos de Meses
MONTH_CODES = {1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M', 7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'}

class HistoryIngestor:
    def __init__(self):
        self.base_url = os.getenv("CMA_HOST") + "/execute"
        self.user = os.getenv("CMA_USER")
        self.password = os.getenv("CMA_PASS")
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10)
        self.session.mount('https://', adapter)
        self.session_id = None
        self.msg_id = 0
        self.processed_cache = {} # Evita baixar o mesmo ativo a cada segundo sem necessidade
        self._init_db()

    def _init_db(self):
        try:
            with duckdb.connect(DB_HISTORY) as conn:
                conn.execute("CREATE TABLE IF NOT EXISTS market_history (symbol VARCHAR, date_ref DATE, open DOUBLE, max DOUBLE, min DOUBLE, close DOUBLE, volume DOUBLE)")
        except: pass

    def _post(self, name, payload):
        data = {"id": self.msg_id+1, "name": name, "sessionId": self.session_id or "", **payload}
        self.msg_id += 1
        try:
            resp = self.session.post(self.base_url, data={'JSONRequest': json.dumps(data)}, timeout=15)
            return resp.json() if resp.status_code == 200 else None
        except: return None

    def login(self):
        print("📚 [History] Autenticando...")
        resp = self._post("LoginRequest", {"user": self.user, "pass": self.password, "type": "s", "service": "m", "transport": "Polling", "version": 1, "sync": True, "oms": {"ip": "0.0.0.0", "channel": "API", "language": "PT"}})
        if resp and resp.get("success"):
            self.session_id = resp.get("sessionId")
            return True
        return False

    def get_current_dollar_target(self):
        """Calcula qual é o Dólar atual para manter sempre atualizado"""
        today = datetime.now()
        # Lógica simples: Pega o mês atual. Se for fim de mês, pega o próximo.
        # Para B3, Dólar vence no 1o dia útil. Então dia 2 já é o próximo.
        target_date = today + relativedelta(months=1) if today.day > 1 else today
        m_char = MONTH_CODES[target_date.month]
        y_str = target_date.strftime('%y')
        return {"symbol": f"DOL{m_char}{y_str}", "sourceId": "57"}

    def get_requested_target(self):
        """Lê o pedido do usuário no arquivo"""
        try:
            if os.path.exists(CHART_FILE):
                with open(CHART_FILE, 'r') as f:
                    data = json.load(f)
                    if data.get('symbol'): return data
        except: pass
        return None

    def save_to_db(self, rows, symbol):
        for _ in range(5):
            try:
                con = duckdb.connect(DB_HISTORY)
                # Apaga só o ativo específico
                con.execute(f"DELETE FROM market_history WHERE symbol = '{symbol}'")
                con.executemany("INSERT INTO market_history VALUES (?,?,?,?,?,?,?)", rows)
                con.close()
                return True
            except: time.sleep(0.2)
        return False

    def run(self):
        while True:
            if not self.session_id:
                if not self.login(): time.sleep(5); continue
            
            try:
                # LISTA DE PRIORIDADES:
                # 1. Dólar Atual (Sempre atualiza)
                # 2. Ativo Solicitado (Se diferente do Dólar)
                targets = []
                
                dollar = self.get_current_dollar_target()
                targets.append(dollar)
                
                requested = self.get_requested_target()
                if requested and requested['symbol'] != dollar['symbol']:
                    targets.append(requested)

                for target in targets:
                    sym = target['symbol']
                    
                    # Cache Simples: Se já baixou esse ativo nos últimos 60s, pula
                    last_update = self.processed_cache.get(sym, 0)
                    if time.time() - last_update < 60: # Atualiza a cada 1 min
                        continue

                    print(f"📉 Baixando: {sym}...")
                    payload = {
                        "type": "c", "sync": True, 
                        "symbolId": {"sourceId": target['sourceId'], "symbol": sym},
                        "dateFrom": (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d"),
                        "dateTo": datetime.now().strftime("%Y-%m-%d"), "period": 1
                    }
                    resp = self._post("DailyGraphRequest", payload)
                    
                    rows = []
                    bars = resp.get('graphicalBars') or resp.get('bars') if resp else None
                    
                    if bars:
                        for b in bars:
                            try:
                                rows.append((
                                    sym, str(b.get('date')),
                                    float(b.get('open') or 0), float(b.get('max') or b.get('high') or b.get('04') or 0),
                                    float(b.get('min') or b.get('low') or b.get('05') or 0), float(b.get('close') or 0),
                                    float(b.get('volume') or 0)
                                ))
                            except: continue
                    
                    if rows:
                        if self.save_to_db(rows, sym):
                            print(f"✅ {sym}: {len(rows)} candles salvos.")
                            self.processed_cache[sym] = time.time()
                    else:
                        print(f"⚠️ {sym}: Sem dados.")
                        # Marca como processado para não tentar a cada segundo
                        self.processed_cache[sym] = time.time() 
                
                time.sleep(1)
            
            except Exception as e:
                print(f"❌ Erro: {e}")
                time.sleep(5)

if __name__ == "__main__":
    print("🚀 Ingestor History v6.0 (Dual Target)")
    HistoryIngestor().run()