import json
import time
import websocket
import os
import sys
import numpy as np

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(CURRENT_DIR)
BASE_DIR = os.path.dirname(CURRENT_DIR)

from engine import DecisionEngine, MarketEvent, AdaptiveRegimeModel, Config

def fixed_detect_shock(self):
    effective_vol = self.current_vol if self.current_vol > 1e-9 else 1e-9
    effective_spread = self.current_spread if self.current_spread > 0 else 1e-8
    
    dist = 0.0
    if self.initialized:
        try:
            x = np.array([np.log(effective_vol), np.log(effective_spread)])
            delta = x - self.mu
            dist = np.sqrt(max(0, delta.T @ self.inv_cov @ delta))
        except:
            dist = 0.0

    self.dist_history.append(dist)
    
    if len(self.dist_history) < 20: 
        return dist, False, "GATHERING_DATA"
        
    mean_dist = np.mean(self.dist_history)
    std_dist = np.std(self.dist_history)
    dynamic_threshold = mean_dist + (Config.SIGMA_MULTIPLIER * std_dist)
    
    return dist, dist > dynamic_threshold, f"Dist:{dist:.2f}"

AdaptiveRegimeModel.detect_shock = fixed_detect_shock

BASE_OUTPUT = "/output" if os.path.exists("/output") else os.path.join(BASE_DIR, "output")
OUTPUT_DIR = os.path.join(BASE_OUTPUT, "realtime")
CONFIG_PATH = os.path.join(BASE_OUTPUT, "model_config.json")

os.makedirs(OUTPUT_DIR, exist_ok=True)

WS_URL = "wss://fstream.binance.com/stream?streams=btcusdt@aggTrade/btcusdt@depth5@100ms/btcusdt@forceOrder/btcusdt@bookTicker"

def run_realtime():
    print(f"Realtime Mode Started")
    print(f"Output Dir: {OUTPUT_DIR}")
    
    engine = DecisionEngine(config_path=CONFIG_PATH)
    
    f_dec = open(os.path.join(OUTPUT_DIR, 'decisions.jsonl'), 'a')
    f_trans = open(os.path.join(OUTPUT_DIR, 'state_transitions.jsonl'), 'a')

    if not hasattr(run_realtime, "stats"):
        run_realtime.last_state = "BOOTSTRAP"
        run_realtime.stats = {"processed": 0, "blocked": 0, "start_time": int(time.time())}

    def save_summary():
        summary_path = os.path.join(OUTPUT_DIR, "summary.json")
        summary_data = {
            "timestamp": int(time.time()),
            "duration_sec": int(time.time()) - run_realtime.stats["start_time"],
            "total_events": run_realtime.stats["processed"],
            "blocked_events": run_realtime.stats["blocked"],
            "final_state": run_realtime.last_state
        }
        with open(summary_path, 'w') as f:
            json.dump(summary_data, f, indent=4)
        print(f"\n[Summary] Saved to {summary_path}")

    def on_message(ws, message):
        try:
            msg = json.loads(message)
            if 'stream' not in msg: return
            data, now_ms = msg['data'], int(time.time() * 1000)
            
            events_to_process = []
            
            if 'aggTrade' in msg['stream']: 
                data['price'] = data['p']
                data['id'] = data['a']
                events_to_process.append(MarketEvent(event_time=data['T'], local_time=now_ms, type="TRADE", data=data))
            
            elif 'depth' in msg['stream']: 
                data['bids'] = data['b']
                data['asks'] = data['a']
                events_to_process.append(MarketEvent(event_time=data['E'], local_time=now_ms, type="ORDERBOOK", data=data))
            
            elif 'forceOrder' in msg['stream']: 
                events_to_process.append(MarketEvent(event_time=data['E'], local_time=now_ms, type="LIQUIDATION", data=data))
            
            elif 'bookTicker' in msg['stream']:
                new_bid = float(data['b'])
                new_ask = float(data['a'])
                
                curr_bid = engine.model.best_bid
                curr_ask = engine.model.best_ask
                
                ev_ask = MarketEvent(event_time=data['E'], local_time=now_ms, type="ORDERBOOK", data={'side': 'ask', 'price': new_ask})
                ev_bid = MarketEvent(event_time=data['E'], local_time=now_ms, type="ORDERBOOK", data={'side': 'bid', 'price': new_bid})
                
                if curr_ask > 0 and new_bid >= curr_ask:
                    events_to_process = [ev_ask, ev_bid]
                elif curr_bid > 0 and new_ask <= curr_bid:
                    events_to_process = [ev_bid, ev_ask]
                else:
                    events_to_process = [ev_ask, ev_bid]

            for event in events_to_process:
                result = engine.process_event(event)
                run_realtime.stats["processed"] += 1
                
                if result['action'] != "ALLOWED":
                    run_realtime.stats["blocked"] += 1
                    f_dec.write(json.dumps({k: v for k, v in result.items() if not k.startswith('_')}) + "\n")
                    f_dec.flush()
                    
                    if run_realtime.stats["blocked"] % 50 == 0:
                        reason_display = result['reason'] if result['reason'] else "GATHERING_DATA"
                        print(f"\r[BLOCK] {reason_display} | State: {result['_internal_state']}", end="")
                
                curr_state = result['_internal_state']
                if curr_state != run_realtime.last_state:
                    print(f"\nState Transition: {run_realtime.last_state} -> {curr_state}")
                    trust, hypo = engine.get_state_info()
                    trans_log = {"ts": result['ts'], "data_trust": trust, "hypothesis": hypo, "decision": result['action'], "trigger": result['_trigger_detail']}
                    f_trans.write(json.dumps(trans_log) + "\n"); f_trans.flush()
                    run_realtime.last_state = curr_state

        except Exception as e:
            pass

    def on_error(ws, error):
        print(f"\n[Connection Error] {error}")

    def on_close(ws, close_status_code, close_msg):
        print("\n[Connection Closed] Reconnecting in 3 seconds...")

    try:
        while True:
            try:
                ws = websocket.WebSocketApp(
                    WS_URL, 
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close
                )
                ws.run_forever(ping_interval=60, ping_timeout=10)
            except Exception as e:
                print(f"\n[Critical Error] {e}")
            
            time.sleep(3) 
    except KeyboardInterrupt:
        print("\nManually stopped.")
    finally:
        f_dec.close()
        f_trans.close()
        save_summary()

if __name__ == "__main__":
    run_realtime()
