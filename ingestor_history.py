import os
import json
import time
import requests
import duckdb
from datetime import datetime, timedelta
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))
DB_HISTORY = os.path.join(BASE_DIR, 'cma_history.duckdb')
CHART_FILE = os.path.join(BASE_DIR, 'active_chart.json')
DEFAULT_TARGET = {"symbol": "DOLF26", "sourceId": "57"}

class HistoryIngestor:
    def __init__(self):
        self.base_url = os.getenv("CMA_HOST") + "/execute"
        self.user = os.getenv("CMA_USER")
        self.password = os.getenv("CMA_PASS")
        self.session = requests.Session()
        self.session_id = None
        self.msg_id = 0
        self.last_symbol = None
        self.error_count = 0
        self._init_db()

    def _init_db(self):
        try:
            with duckdb.connect(DB_HISTORY) as conn:
                # CRIANDO COM NOMES CERTOS: max e min
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

    def get_target(self):
        try:
            if os.path.exists(CHART_FILE):
                with open(CHART_FILE, 'r') as f: return json.load(f)
        except: pass
        return DEFAULT_TARGET

    def save_to_db(self, rows):
        for _ in range(5):
            try:
                con = duckdb.connect(DB_HISTORY)
                con.execute("DELETE FROM market_history")
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
                target = self.get_target()
                if target['symbol'] != self.last_symbol:
                    if self.error_count >= 3:
                        try: os.remove(CHART_FILE); self.error_count=0
                        except: pass
                        continue

                    print(f"📉 Baixando: {target['symbol']}...")
                    dt_from = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")
                    dt_to = datetime.now().strftime("%Y-%m-%d")
                    
                    resp = self._post("DailyGraphRequest", {"type": "c", "sync": True, "symbolId": {"sourceId": target['sourceId'], "symbol": target['symbol']}, "dateFrom": dt_from, "dateTo": dt_to, "period": 1})
                    
                    if not resp:
                        self.error_count += 1; time.sleep(2); continue

                    rows = []
                    bars = resp.get('graphicalBars') or resp.get('bars')
                    if bars:
                        for b in bars:
                            try:
                                rows.append((
                                    target['symbol'], str(b.get('date')),
                                    float(b.get('open') or 0), 
                                    float(b.get('max') or b.get('high') or b.get('04') or 0),
                                    float(b.get('min') or b.get('low') or b.get('05') or 0),
                                    float(b.get('close') or 0), float(b.get('volume') or 0)
                                ))
                            except: continue
                    
                    if rows:
                        self.save_to_db(rows)
                        print(f"✅ Histórico Salvo: {len(rows)} candles.")
                        self.last_symbol = target['symbol']
                        self.error_count = 0
                    else:
                        print(f"⚠️ Vazio: {target['symbol']}")
                        self.error_count += 1
                        time.sleep(1)
                time.sleep(1)
            except Exception as e: time.sleep(5)

if __name__ == "__main__":
    HistoryIngestor().run()