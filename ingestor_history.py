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

    # --- FUNÇÃO NOVA: DESTRUIÇÃO DE SESSÃO INVÁLIDA ---
    def invalidate_session(self):
        print("🔥 [History] Sessão Inválida detectada! Apagando token...")
        self.session_id = None
        try:
            if os.path.exists(SESSION_FILE):
                os.remove(SESSION_FILE)
        except: pass

    def login(self):
        print("📚 [History] Verificando sessão...")
        shared_id = self.get_shared_session()
        if shared_id and shared_id != self.session_id:
            print(f"♻️ Usando sessão compartilhada: {shared_id[:10]}...")
            self.session_id = shared_id
            return True
        
        print("📚 [History] Autenticando...")
        self.session.cookies.clear()
        self.msg_id += 1
        payload = {"id": self.msg_id, "name": "LoginRequest", "sessionId": "", "user": self.user, "pass": self.password, "type": "s", "service": "m", "transport": "Polling", "version": 1, "sync": True, "oms": {"ip": "0.0.0.0", "channel": "API", "language": "PT"}}
        try:
            resp = self.session.post(self.base_url, data={'JSONRequest': json.dumps(payload)}, timeout=10)
            if resp.status_code == 200:
                rj = resp.json()
                if rj.get("success"):
                    self.session_id = rj.get("sessionId")
                    self.save_shared_session(self.session_id)
                    self.error_count = 0
                    print(f"✅ [History] Nova Sessão: {self.session_id[:10]}...")
                    return True
        except Exception as e: print(f"❌ Login Erro: {e}")
        return False

    def _post(self, name, payload):
        self.msg_id += 1
        data = {"id": self.msg_id, "name": name, "sessionId": self.session_id or "", **payload}
        try:
            resp = self.session.post(self.base_url, data={'JSONRequest': json.dumps(data)}, timeout=20)
            if resp.status_code == 200: return resp.json()
            return None
        except: return None

    def get_current_dollar_target(self):
        today = datetime.now()
        target_date = today + relativedelta(months=1) if today.day > 1 else today
        m_char = MONTH_CODES[target_date.month]
        y_str = target_date.strftime('%y')
        return {"symbol": f"DOL{m_char}{y_str}", "sourceId": "57"}

    def get_requested_target(self):
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
                con.execute(f"DELETE FROM market_history WHERE symbol = '{symbol}'")
                if rows: con.executemany("INSERT INTO market_history VALUES (?,?,?,?,?,?,?)", rows)
                con.close()
                return True
            except: time.sleep(0.2)
        return False

    def run(self):
        print("🚀 [History] Ingestor Iniciado (v13.0 - Self Healing)...")
        while True:
            if self.error_count > 10:
                print("💀 [History] Muitos erros. Reiniciando...")
                self.invalidate_session() # Tenta limpar antes de morrer
                sys.exit(1)

            if not self.session_id:
                if not self.login(): 
                    self.error_count += 1
                    time.sleep(10)
                    continue
            
            try:
                targets = []
                dollar = self.get_current_dollar_target()
                targets.append(dollar)
                requested = self.get_requested_target()
                if requested and requested['symbol'] != dollar['symbol']: targets.append(requested)

                for target in targets:
                    sym = target['symbol']
                    sid = target.get('sourceId', '57')
                    
                    last_update = self.processed_cache.get(sym, 0)
                    if time.time() - last_update < 60: continue

                    print(f"📉 Baixando: {sym} (Source: {sid})...", end=" ")
                    
                    payload = {"type": "c", "sync": True, "symbolId": {"sourceId": sid, "symbol": sym}, "dateFrom": (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d"), "dateTo": datetime.now().strftime("%Y-%m-%d"), "period": 1}
                    
                    resp = self._post("DailyGraphRequest", payload)
                    
                    # --- LÓGICA DE DETECÇÃO DE SESSÃO MORTA ---
                    if resp and not resp.get("success"):
                        err_status = resp.get("status", 0)
                        # Status 10004 = Sessão Inválida/Expirada
                        if err_status == 10004 or "session" in str(resp).lower():
                            print(f"\n❌ SESSÃO MORREU ({err_status}). Resetando...")
                            self.invalidate_session()
                            break # Sai do loop de targets para relogar imediatamente
                        else:
                            print(f"\n⚠️ API Recusou {sym}: {resp.get('textual')}")
                            self.processed_cache[sym] = time.time()
                            continue

                    if not resp:
                        print("❌ Falha Rede")
                        self.error_count += 1
                        continue

                    self.error_count = 0
                    rows = []
                    bars = resp.get('graphicalBars') or resp.get('bars')
                    if bars:
                        for b in bars:
                            try:
                                rows.append((sym, str(b.get('date')), float(b.get('open') or 0), float(b.get('max') or 0), float(b.get('min') or 0), float(b.get('close') or 0), float(b.get('volume') or 0)))
                            except: continue
                    
                    if rows:
                        if self.save_to_db(rows, sym):
                            print(f"✅ {len(rows)} candles.")
                            self.processed_cache[sym] = time.time()
                    else:
                        print(f"⚠️ Sem dados.")
                        self.processed_cache[sym] = time.time()
                time.sleep(1)
            
            except Exception as e:
                print(f"❌ Erro Geral: {e}")
                self.error_count += 1
                if self.error_count > 2:
                    self.invalidate_session() # Força reset se errar muito
                time.sleep(5)

if __name__ == "__main__":
    HistoryIngestor().run()