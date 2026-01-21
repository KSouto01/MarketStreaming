import os
import json
import time
import requests
import sys
import duckdb
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))
DB_HISTORY = os.path.join(BASE_DIR, 'cma_history.duckdb')
SESSION_FILE = os.path.join(BASE_DIR, 'session_token.json')
CHART_FILE = os.path.join(BASE_DIR, 'active_chart.json')

MONTH_CODES = {1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M', 7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'}

class HistoryIngestor:
    def __init__(self):
        host = os.getenv("CMA_HOST")
        if not host: sys.exit("❌ HOST Vazio")
        self.base_url = host + "/execute"
        self.user = os.getenv("CMA_USER")
        self.password = os.getenv("CMA_PASS")
        self.session = requests.Session()
        self.session.mount('https://', requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10))
        self.session_id = None
        self.msg_id = 0
        self.error_count = 0
        self.processed_cache = {}
        self._init_db()

    def _init_db(self):
        try:
            with duckdb.connect(DB_HISTORY) as conn:
                conn.execute("CREATE TABLE IF NOT EXISTS market_history (symbol VARCHAR, date_ref DATE, open DOUBLE, max DOUBLE, min DOUBLE, close DOUBLE, volume DOUBLE)")
        except: pass

    def get_shared_session(self):
        try:
            if os.path.exists(SESSION_FILE):
                with open(SESSION_FILE, 'r') as f: return json.load(f).get('sessionId')
        except: pass
        return None

    def save_shared_session(self, new_id):
        try:
            with open(SESSION_FILE, 'w') as f: json.dump({'sessionId': new_id, 'updated_at': time.time()}, f)
        except: pass

    def invalidate_session(self):
        print("🔥 [History] Sessão Inválida! Resetando...")
        self.session_id = None
        try:
            if os.path.exists(SESSION_FILE): os.remove(SESSION_FILE)
        except: pass

    def login(self):
        print("📚 [History] Autenticando...")
        shared_id = self.get_shared_session()
        if shared_id and shared_id != self.session_id:
            self.session_id = shared_id
            return True
        
        self.session.cookies.clear()
        self.msg_id += 1
        payload = {"id": self.msg_id, "name": "LoginRequest", "sessionId": "", "user": self.user, "pass": self.password, "type": "s", "service": "m", "transport": "Polling", "version": 1, "sync": True, "oms": {"ip": "0.0.0.0", "channel": "API", "language": "PT"}}
        try:
            resp = self.session.post(self.base_url, data={'JSONRequest': json.dumps(payload)}, timeout=10)
            if resp.status_code == 200 and resp.json().get("success"):
                self.session_id = resp.json().get("sessionId")
                self.save_shared_session(self.session_id)
                self.error_count = 0
                return True
        except: pass
        return False

    def _post(self, name, payload):
        self.msg_id += 1
        data = {"id": self.msg_id, "name": name, "sessionId": self.session_id or "", **payload}
        try:
            resp = self.session.post(self.base_url, data={'JSONRequest': json.dumps(data)}, timeout=20)
            if resp.status_code == 200: return resp.json()
        except: pass
        return None

    def get_user_request(self):
        for _ in range(3): 
            try:
                if os.path.exists(CHART_FILE):
                    with open(CHART_FILE, 'r') as f: return json.load(f)
            except: time.sleep(0.1)
        return None

    def get_dollar_targets(self):
        targets = []
        today = datetime.now()
        for i in range(2): 
            target_date = today + relativedelta(months=i)
            if today.day > 25: target_date = today + relativedelta(months=i+1)
            m_char = MONTH_CODES[target_date.month]
            y_str = target_date.strftime('%y')
            # 1 ANO DE DADOS PARA O DÓLAR TAMBÉM
            targets.append({"symbol": f"DOL{m_char}{y_str}", "sourceId": "57", "days": 365, "priority": "low"})
        return targets

    def save_to_db(self, rows, symbol):
        for _ in range(5):
            try:
                con = duckdb.connect(DB_HISTORY)
                con.execute(f"DELETE FROM market_history WHERE symbol = '{symbol}'")
                if rows: con.executemany("INSERT INTO market_history VALUES (?,?,?,?,?,?,?)", rows)
                con.close()
                return True
            except: time.sleep(0.2)
        return False

    def run(self):
        print("🚀 [History] Ingestor Iniciado (v22.0 - 1 Year Standard)...")
        while True:
            if self.error_count > 10:
                self.invalidate_session()
                sys.exit(1)

            if not self.session_id:
                if not self.login(): 
                    self.error_count += 1
                    time.sleep(10)
                    continue
            
            try:
                targets_map = {}
                for d in self.get_dollar_targets():
                    targets_map[d['symbol']] = d

                user_req = self.get_user_request()
                
                if user_req and user_req.get('symbol'):
                    req_sym = user_req.get('symbol')
                    req_source = user_req.get('sourceId', '57')
                    
                    # SEMPRE 365 DIAS (1 ANO) INDEPENDENTE DO FILTRO
                    # Isso permite ao frontend fazer zoom local sem chamar API
                    days_needed = 365 
                    
                    if req_sym in targets_map:
                        targets_map[req_sym]['days'] = days_needed
                        targets_map[req_sym]['priority'] = "high"
                    else:
                        targets_map[req_sym] = {"symbol": req_sym, "sourceId": req_source, "days": days_needed, "priority": "high"}

                for sym, target in targets_map.items():
                    sid = target.get('sourceId', '57')
                    days = target.get('days', 365)
                    priority = target.get('priority', 'low')
                    
                    cache_key = f"{sym}_{days}"
                    last_update = self.processed_cache.get(cache_key, 0)
                    ttl = 5 if priority == "high" else 60
                    if time.time() - last_update < ttl: continue

                    print(f"📉 Baixando: {sym} ({days}d)...", end=" ")
                    
                    payload = {"type": "c", "sync": True, "symbolId": {"sourceId": sid, "symbol": sym}, "dateFrom": (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d"), "dateTo": datetime.now().strftime("%Y-%m-%d"), "period": 1}
                    
                    resp = self._post("DailyGraphRequest", payload)
                    
                    if resp and not resp.get("success"):
                        if resp.get("status") == 10004:
                            self.invalidate_session()
                            break 
                        else:
                            print(f"⚠️ API Error")
                            self.processed_cache[cache_key] = time.time()
                            continue

                    if not resp:
                        self.error_count += 1
                        continue

                    rows = []
                    bars = resp.get('graphicalBars') or resp.get('bars')
                    if bars:
                        for b in bars:
                            try:
                                rows.append((sym, str(b.get('date')), float(b.get('open') or 0), float(b.get('max') or 0), float(b.get('min') or 0), float(b.get('close') or 0), float(b.get('volume') or 0)))
                            except: continue
                    
                    if self.save_to_db(rows, sym):
                        print(f"✅")
                        self.processed_cache[cache_key] = time.time()
                
                time.sleep(1)
            except Exception as e:
                self.error_count += 1
                time.sleep(2)

if __name__ == "__main__":
    HistoryIngestor().run()