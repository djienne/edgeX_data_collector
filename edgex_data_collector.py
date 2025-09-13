#!/usr/bin/env python3
"""
EdgeX Market Data Collector - FINAL STABLE VERSION
Focuses on reliably collecting Ticker, Trades, and Order Book (depth) data streams.
Processes order book snapshots and changes to create a full snapshot for each update.
Debug logging is on by default.
"""

import asyncio
import json
import time
import os
import csv
import logging
from datetime import datetime
from collections import defaultdict, deque
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor
import websockets

# --- Logger Setup ---
logger = logging.getLogger(__name__)

# --- Data Models ---
@dataclass
class PriceData:
    timestamp: float; contract_id: str; price: float
    size: float = 0; side: Optional[str] = None; exchange_timestamp: Optional[int] = None

@dataclass
class TradeData:
    timestamp: float; contract_id: str; price: float; size: float; side: str
    ticketId: Optional[str] = None; exchange_timestamp: Optional[int] = None

# --- Statistics Tracker ---
class DataStats:
    def __init__(self):
        self.start_time = time.time()
        self.counters = defaultdict(int)
    
    def update(self, data_type: str, count: int = 1):
        self.counters[data_type] += count
    
    def get_summary(self) -> Dict[str, Any]:
        runtime = time.time() - self.start_time
        return {
            'runtime_formatted': f"{runtime//3600:.0f}h {(runtime%3600)//60:.0f}m {runtime%60:.0f}s",
            'counters': dict(self.counters),
            'rates_per_minute': {k: v / (runtime / 60) for k, v in self.counters.items() if runtime > 0},
            'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

# --- Main Collector Class ---
class EdgeXDataCollector:
    """Main data collector class for EdgeX - FINAL STABLE VERSION"""
    
    def __init__(self, contract_map: Dict[str, str], output_dir: str, ws_url: str):
        self.contract_map = contract_map
        self.contract_ids = list(contract_map.keys())
        self.output_dir = output_dir
        self.ws_url = ws_url
        self.stats = DataStats()
        self.depth_level = 15 # The depth level to subscribe to
        
        self.running = asyncio.Event()
        self._tasks: List[asyncio.Task] = []
        self.executor = ThreadPoolExecutor(max_workers=len(self.contract_ids) * 3)
        
        self.data_buffers = {cid: {'tickers': deque(), 'trades': deque(), 'depth': deque()} for cid in self.contract_ids}
        self.contract_files = {}
        self.order_books = {cid: {'bids': {}, 'asks': {}} for cid in self.contract_ids}
        
        self.last_ticker_state: Dict[str, Dict] = {}

        logger.info("Initializing collector with debug logging enabled.")
        os.makedirs(output_dir, exist_ok=True)
        self._init_csv_files()

    def _init_csv_files(self):
        for contract_id in self.contract_ids:
            ticker_name = self.contract_map.get(contract_id, contract_id)
            self.contract_files[contract_id] = {}
            
            # Create dynamic headers for the depth file
            depth_headers = ['timestamp']
            for i in range(1, self.depth_level + 1):
                depth_headers.append(f'bid_{i}_price')
                depth_headers.append(f'bid_{i}_size')
            for i in range(1, self.depth_level + 1):
                depth_headers.append(f'ask_{i}_price')
                depth_headers.append(f'ask_{i}_size')

            headers = {
                'tickers': ['timestamp', 'contract_id', 'price', 'size', 'side', 'exchange_timestamp'],
                'trades': ['timestamp', 'contract_id', 'price', 'size', 'side', 'ticketId', 'exchange_timestamp'],
                'depth': depth_headers,
            }

            for data_type, header_list in headers.items():
                path = os.path.join(self.output_dir, f"{data_type}_{ticker_name}.csv")
                self.contract_files[contract_id][data_type] = path
                logger.info(f"Initialized CSV file: {path}")
                if not os.path.exists(path):
                    with open(path, 'w', newline='') as f:
                        csv.writer(f).writerow(header_list)
    
    def _write_to_csv_sync(self, filename: str, data: List[Dict]):
        if not data: return
        logger.debug(f"WRITING {len(data)} rows to {os.path.basename(filename)}")
        try:
            headers = list(data[0].keys())
            file_exists = os.path.isfile(filename) and os.path.getsize(filename) > 0
            with open(filename, 'a', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                if not file_exists:
                    writer.writeheader()
                writer.writerows(data)
        except Exception:
            logger.error(f"Error writing to CSV {filename}", exc_info=True)

    async def _flush_buffers(self):
        loop = asyncio.get_running_loop()
        logger.debug("Starting periodic buffer flush...")
        for contract_id, buffers in self.data_buffers.items():
            for data_type, buffer in buffers.items():
                if buffer:
                    data_to_write = list(buffer)
                    buffer.clear()
                    filename = self.contract_files[contract_id][data_type]
                    await loop.run_in_executor(self.executor, self._write_to_csv_sync, filename, data_to_write)
        logger.debug("Buffer flush complete.")
    
    def _handle_message(self, message: str):
        try:
            data = json.loads(message)
            pretty_payload = json.dumps(data, indent=2)
            logger.debug(f"RAW MESSAGE RECEIVED:\n{pretty_payload}")
            
            if data.get('type') == 'subscribed':
                logger.info(f"SUCCESSFULLY SUBSCRIBED to channel: {data.get('channel')}")
                return
            elif data.get('type') == 'error':
                logger.error(f"SERVER ERROR on subscription: {data.get('content', {}).get('msg')} for request: {data.get('request')}")
                return
            
            channel = data.get('channel', '')
            if 'ticker' in channel: self._process_ticker(data)
            elif 'trades' in channel: self._process_trade(data)
            elif 'depth' in channel: self._process_depth(data)
            else: logger.debug(f"Ignoring message with type '{data.get('type')}' and channel: '{channel}'")
        except json.JSONDecodeError:
            logger.warning(f"RAW MESSAGE (NOT JSON): {message[:300]}")
        except Exception:
            logger.error(f"Error processing message: {message[:300]}", exc_info=True)
            
    def _process_ticker(self, msg: Dict):
        contract_id = msg['channel'].split('.')[1]
        if contract_id not in self.contract_ids: return
        
        update_data = msg.get('content', {}).get('data', [{}])[0]
        
        last_state = self.last_ticker_state.get(contract_id, {})
        last_state.update(update_data)
        self.last_ticker_state[contract_id] = last_state
        current_data = last_state

        now = time.time()
        exchange_ts = current_data.get('timestamp')
        records_to_add = []

        last_price = next((float(current_data[k]) for k in ['lastPrice', 'close'] if current_data.get(k) is not None), None)
        bid_price = next((float(current_data[k]) for k in ['bestBidPrice', 'bid'] if current_data.get(k) is not None), None)
        ask_price = next((float(current_data[k]) for k in ['bestAskPrice', 'ask'] if current_data.get(k) is not None), None)
        
        if last_price:
            records_to_add.append(asdict(PriceData(now, contract_id, last_price, 0, 'last', exchange_ts)))
        if bid_price:
            records_to_add.append(asdict(PriceData(now, contract_id, bid_price, 0, 'bid', exchange_ts)))
        if ask_price:
            records_to_add.append(asdict(PriceData(now, contract_id, ask_price, 0, 'ask', exchange_ts)))
        
        if records_to_add:
            logger.debug(f"PROCESSED TICKER GROUP for {contract_id}: last={last_price}, bid={bid_price}, ask={ask_price}")
            self.data_buffers[contract_id]['tickers'].extend(records_to_add)
            self.stats.update('ticker_datapoints', len(records_to_add))

    def _process_trade(self, msg: Dict):
        contract_id = msg['channel'].split('.')[1]
        if contract_id not in self.contract_ids: return
        for data in msg.get('content', {}).get('data', []):
            price_val = data.get('price')
            size_val = data.get('size')
            
            side_val = 'unknown'
            if isinstance(data.get('isBuyerMaker'), bool):
                side_val = 'sell' if data['isBuyerMaker'] else 'buy'

            if price_val is None or size_val is None: continue
            
            ticket_id = str(data.get('ticketId', ''))
            exchange_ts = data.get('time')

            self.data_buffers[contract_id]['trades'].append(asdict(TradeData(
                time.time(), 
                contract_id, 
                float(price_val), 
                float(size_val), 
                side_val, 
                ticket_id, 
                exchange_ts
            )))
            self.stats.update('trade_updates')

    def _format_book_to_row(self, contract_id: str) -> Dict:
        book = self.order_books[contract_id]
        row = {'timestamp': time.time()}

        sorted_bids = sorted(book['bids'].items(), key=lambda item: float(item[0]), reverse=True)
        sorted_asks = sorted(book['asks'].items(), key=lambda item: float(item[0]))

        for i in range(self.depth_level):
            if i < len(sorted_bids):
                price, size = sorted_bids[i]
                row[f'bid_{i+1}_price'] = price
                row[f'bid_{i+1}_size'] = size
            else:
                row[f'bid_{i+1}_price'] = None
                row[f'bid_{i+1}_size'] = None

            if i < len(sorted_asks):
                price, size = sorted_asks[i]
                row[f'ask_{i+1}_price'] = price
                row[f'ask_{i+1}_size'] = size
            else:
                row[f'ask_{i+1}_price'] = None
                row[f'ask_{i+1}_size'] = None
                
        return row

    def _process_depth(self, msg: Dict):
        channel_parts = msg['channel'].split('.')
        contract_id = channel_parts[1]
        if contract_id not in self.contract_ids: return

        book = self.order_books[contract_id]

        for data_item in msg.get('content', {}).get('data', []):
            depth_type = data_item.get('depthType')

            if depth_type == 'Snapshot':
                book['bids'].clear()
                book['asks'].clear()

            for level in data_item.get('bids', []):
                price, size = level.get('price'), level.get('size')
                if price is None or size is None: continue
                if float(size) == 0:
                    book['bids'].pop(price, None)
                else:
                    book['bids'][price] = size

            for level in data_item.get('asks', []):
                price, size = level.get('price'), level.get('size')
                if price is None or size is None: continue
                if float(size) == 0:
                    book['asks'].pop(price, None)
                else:
                    book['asks'][price] = size
            
            snapshot_row = self._format_book_to_row(contract_id)
            self.data_buffers[contract_id]['depth'].append(snapshot_row)
            self.stats.update('depth_snapshots')
    
    async def _periodic_flush_task(self):
        while self.running.is_set(): await asyncio.sleep(10); await self._flush_buffers()

    async def _periodic_summary_task(self):
        while self.running.is_set(): await asyncio.sleep(30); self._print_summary()

    async def _run_collection_task(self):
        data_types = ["ticker", "trades"]
        channels = [f"{dtype}.{cid}" for cid in self.contract_ids for dtype in data_types]
        
        depth_channels = [f"depth.{cid}.{self.depth_level}" for cid in self.contract_ids]
        channels.extend(depth_channels)

        while self.running.is_set():
            try:
                connect_url = f"{self.ws_url}?timestamp={int(time.time() * 1000)}"
                logger.info(f"Connecting to {connect_url}...")
                
                async with websockets.connect(connect_url, ping_interval=20, ping_timeout=20) as ws:
                    logger.info("Connection successful. Subscribing to channels individually...")
                    for channel in channels:
                        sub_msg_str = json.dumps({"type": "subscribe", "channel": channel})
                        logger.debug(f"SENDING SUBSCRIBE MESSAGE: {sub_msg_str}")
                        await ws.send(sub_msg_str)
                        await asyncio.sleep(0.1)
                    logger.info(f"All {len(channels)} subscriptions sent. Awaiting data...")
                    async for message in ws: self._handle_message(message)
            except (websockets.exceptions.ConnectionClosed, websockets.exceptions.InvalidStatusCode) as e:
                logger.warning(f"WebSocket issue: {e}. Reconnecting in 5s...")
            except asyncio.TimeoutError:
                logger.warning("Connection timed out. Reconnecting in 5s...")
            except Exception as e:
                logger.error(f"Unexpected error in collection loop: {e}", exc_info=True)
            if self.running.is_set(): await asyncio.sleep(5)

    def _print_summary(self):
        summary = self.stats.get_summary()
        print("\n" + "="*60); print(f"EDGEX DATA COLLECTION SUMMARY - {summary['last_update']}")
        print("="*60); print(f"Runtime: {summary['runtime_formatted']}")
        for dtype, count in summary['counters'].items():
            rate = summary['rates_per_minute'].get(dtype, 0)
            print(f"  {dtype:<20}: {count:>10,} ({rate:>6.1f}/min)")
        print("="*60)

    async def _run_main_loop(self):
        self.running.set()
        self._tasks = [asyncio.create_task(self._periodic_flush_task()), asyncio.create_task(self._periodic_summary_task()), asyncio.create_task(self._run_collection_task())]
        await asyncio.gather(*self._tasks, return_exceptions=True)

    def start(self):
        tickers = ", ".join(self.contract_map.values())
        print(f"Starting EdgeX data collection for: {tickers}")
        try: asyncio.run(self._run_main_loop())
        except KeyboardInterrupt: print("\nShutdown signal received.")
        finally: self.stop()
            
    def stop(self):
        if not self.running.is_set(): return
        print("\nInitiating graceful shutdown...")
        self.running.clear()
        
        for task in self._tasks: task.cancel() 
        
        logger.info("Flushing final data buffers...")
        for cid, buffers in self.data_buffers.items():
            for dtype, buffer in buffers.items():
                if buffer: self._write_to_csv_sync(self.contract_files[cid][dtype], list(buffer))
        self.executor.shutdown(wait=True)
        
        self._print_summary()
        print(f"Data collection stopped. Files saved in: {self.output_dir}")

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    CONTRACT_MAP = {
        "10000001": "BTC",
        "10000002": "ETH",
        "10000003": "SOL",
        "10000227": "PAXG"
    }
    
    OUTPUT_DIR = "edgex_data"
    WS_URL = "wss://quote.edgex.exchange/api/v1/public/ws"

    collector = EdgeXDataCollector(
        contract_map=CONTRACT_MAP,
        output_dir=OUTPUT_DIR,
        ws_url=WS_URL,
    )
    collector.start()

if __name__ == "__main__":
    main()

