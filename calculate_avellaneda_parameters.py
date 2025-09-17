# Avellaneda-Stoikov Market Making Model Parameter Calculator
# This script implements the optimal market making strategy from
# "High-frequency trading in a limit order book" by Avellaneda & Stoikov (2008)

import numpy as np
import pandas as pd
import scipy.optimize
from scipy.optimize import brentq, fsolve
import sys
import os
import argparse
from pathlib import Path
import warnings
from numba import jit
import json

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Calculate Avellaneda-Stoikov market making parameters')
    parser.add_argument('ticker', nargs='?', default='BTC', help='Ticker symbol (default: BTC)')
    parser.add_argument('--hours', type=int, default=24, help='Frequency in hours to recalculate parameters (default: 24)')
    return parser.parse_args()

def get_tick_size(ticker):
    """Get tick size based on the ticker symbol."""
    if ticker == 'BTC':
        return 1.0
    elif ticker == 'ETH':
        return 0.1
    elif ticker == 'SOL':
        return 0.01
    elif ticker == 'WLFI':
        return 0.0001
    elif ticker == 'PAXG':
        return 0.01
    else:
        return 0.01

def load_trades_data(csv_path):
    """Load trades data from a CSV file."""
    df = pd.read_csv(csv_path)
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
    df = df.set_index('datetime')
    return df

def load_and_resample_mid_price(csv_path):
    """Load and resample mid-price data from a CSV file."""
    df = pd.read_csv(csv_path)
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
    pivot_df = df.pivot_table(index='datetime', columns='side', values='price')
    pivot_df.ffill(inplace=True)
    pivot_df.rename(columns={'bid': 'price_bid', 'ask': 'price_ask'}, inplace=True)
    merged = pivot_df.resample('s').ffill()
    merged['mid_price'] = (merged['price_bid'] + merged['price_ask']) / 2
    merged.dropna(inplace=True)
    return merged

def calculate_volatility(mid_price_df, H, freq_str):
    """Calculate rolling volatility (sigma)."""
    print("\n" + "-"*20)
    print("Calculating volatility (sigma)...")
    
    num_periods = len(mid_price_df.index.floor(freq_str).unique()) -1
    num_days_equivalent = num_periods * H / 24

    rolling_avg_per = '2D'
    if num_days_equivalent > 2: rolling_avg_per = '3D'
    if num_days_equivalent > 3: rolling_avg_per = '4D'
    if num_days_equivalent > 4: rolling_avg_per = '5D'
    if num_days_equivalent > 5: rolling_avg_per = '6D'
    if num_days_equivalent > 7: rolling_avg_per = '7D'

    std = (np.log(mid_price_df.loc[:, 'mid_price']).diff()
           .dropna()
           .groupby(pd.Grouper(freq=freq_str)).std()
           .rolling(rolling_avg_per).mean())
    
    sigma_list = (std * np.sqrt(60 * 60 * 24)).tolist()
    sigma_list = sigma_list[:-1]
    
    if sigma_list:
        print("Latest sigma values:")
        for s in sigma_list[-3:]:
            print(f"  - {s:.6f}")
    else:
        print("Sigma values not available.")
    return sigma_list

def calculate_intensity_params(list_of_periods, H, buy_orders, sell_orders, deltalist, mid_price_df):
    """Calculate order arrival intensity parameters (A and k)."""
    print("\n" + "-"*20)
    print("Calculating order arrival intensity (A and k)...")

    def exp_fit(x, a, b):
        return a * np.exp(-b * x)

    Alist, klist = [], []

    for i in range(len(list_of_periods)):
        period_start = list_of_periods[i]
        period_end = period_start + pd.Timedelta(hours=H)

        mask_buy = (buy_orders.index >= period_start) & (buy_orders.index < period_end)
        period_buy_orders = buy_orders.loc[mask_buy].copy()

        mask_sell = (sell_orders.index >= period_start) & (sell_orders.index < period_end)
        period_sell_orders = sell_orders.loc[mask_sell].copy()

        if period_buy_orders.empty and period_sell_orders.empty:
            Alist.append(float('nan'))
            klist.append(float('nan'))
            continue

        best_bid = period_buy_orders['price'].max() if not period_buy_orders.empty else np.nan
        best_ask = period_sell_orders['price'].min() if not period_sell_orders.empty else np.nan

        if pd.isna(best_bid) or pd.isna(best_ask):
            s_period = mid_price_df.loc[period_start:period_end]
            reference_mid = s_period['mid_price'].mean() if not s_period.empty else np.nan
        else:
            reference_mid = (best_bid + best_ask) / 2

        if pd.isna(reference_mid):
            Alist.append(float('nan'))
            klist.append(float('nan'))
            continue

        deltadict = {}
        for price_delta in deltalist:
            limit_bid = reference_mid - price_delta
            limit_ask = reference_mid + price_delta
            
            bid_hits = []
            if not period_sell_orders.empty:
                sell_hits_bid = period_sell_orders[period_sell_orders['price'] <= limit_bid]
                if not sell_hits_bid.empty:
                    bid_hits = sell_hits_bid.index.tolist()
            
            ask_hits = []
            if not period_buy_orders.empty:
                buy_hits_ask = period_buy_orders[period_buy_orders['price'] >= limit_ask]
                if not buy_hits_ask.empty:
                    ask_hits = buy_hits_ask.index.tolist()
            
            all_hits = sorted(bid_hits + ask_hits)
            
            if len(all_hits) > 1:
                hit_times = pd.DatetimeIndex(all_hits)
                deltas = hit_times.to_series().diff().dt.total_seconds().dropna()
                deltadict[price_delta] = deltas
            else:
                deltadict[price_delta] = pd.Series([H * 3600])

        lambdas = pd.DataFrame({
            "delta": list(deltadict.keys()),
            "lambda_delta": [1 / d.mean() if len(d) > 0 else 1e-6 for d in deltadict.values()]
        }).set_index("delta")

        try:
            paramsB, _ = scipy.optimize.curve_fit(exp_fit, lambdas.index.values, lambdas["lambda_delta"].values, maxfev=5000)
            A, k = paramsB
            Alist.append(A)
            klist.append(k)
        except (RuntimeError, ValueError):
            Alist.append(float('nan'))
            klist.append(float('nan'))

    if Alist and klist:
        print("Latest A and k values:")
        for i in range(max(0, len(Alist) - 3), len(Alist)):
            print(f"  - A: {Alist[i]:.4f}, k: {klist[i]:.6f}")
    else:
        print("A and k values not available.")
    return Alist, klist

def optimize_gamma(list_of_periods, sigma_list, Alist, klist, H, ma_window, mid_price_df, buy_trades, sell_trades, tick_size):
    """Optimize risk aversion parameter (gamma) via backtesting."""
    print("\n" + "-"*20)
    print("Optimizing risk aversion (gamma) via backtesting...")

    GAMMA_CALCULATION_WINDOW = 4
    gammalist = []
    gamma_grid_to_test = None

    start_index = max(1, len(list_of_periods) - GAMMA_CALCULATION_WINDOW)
    period_index_range = range(start_index, len(list_of_periods))

    for j in period_index_range:
        if ma_window > 1:
            a_slice = Alist[max(0, j - ma_window):j]
            k_slice = klist[max(0, j - ma_window):j]
            A = pd.Series(a_slice).mean()
            k = pd.Series(k_slice).mean()
        else:
            A = Alist[j-1]
            k = klist[j-1]

        sigma = sigma_list[j-1]

        if pd.isna(sigma) or pd.isna(A) or pd.isna(k):
            gammalist.append(np.nan)
            continue

        period_start = list_of_periods[j]
        period_end = period_start + pd.Timedelta(hours=H)
        print(f"\nProcessing period: {period_start} to {period_end}")

        mask = (mid_price_df.index >= period_start) & (mid_price_df.index < period_end)
        s_df = mid_price_df.loc[mask]
        s = s_df.resample('s').asfreq(fill_value=np.nan).ffill()['mid_price']

        if s.empty:
            gammalist.append(np.nan)
            continue

        if gamma_grid_to_test is None:
            gamma_grid_to_test = generate_gamma_grid(s.iloc[-1], sigma, k, H)

        if gamma_grid_to_test is None:
            print("Could not find a reasonable gamma interval. Aborting.")
            sys.exit()

        buy_mask = (buy_trades.index >= period_start) & (buy_trades.index < period_end)
        buy_trades_period = buy_trades.loc[buy_mask]
        sell_mask = (sell_trades.index >= period_start) & (sell_trades.index < period_end)
        sell_trades_period = sell_trades.loc[sell_mask]

        gamma_results = []
        for i, gamma_to_test in enumerate(gamma_grid_to_test):
            print(f"  - Testing gamma: {gamma_to_test:.5f} ({i+1}/{len(gamma_grid_to_test)})")
            result = evaluate_gamma(gamma_to_test, s, buy_trades_period, sell_trades_period, k, sigma, H)
            gamma_results.append(result)

        results_df = pd.DataFrame(gamma_results, columns=['gamma', 'pnl', 'spread'])
        valid_results = results_df.dropna(subset=['pnl'])
        
        if valid_results.empty:
            print("Warning: All backtests resulted in NaN or 0 PnL. Using fallback gamma.")
            best_gamma = 0.5
        else:
            positive_pnl_results = valid_results[valid_results['pnl'] > 0]
            if not positive_pnl_results.empty:
                best_gamma = positive_pnl_results.loc[positive_pnl_results['spread'].idxmax()]['gamma']
            else:
                best_gamma = valid_results.loc[valid_results['pnl'].idxmax()]['gamma']
        
        print(f"Best gamma for period: {best_gamma:.5f}")
        gammalist.append(best_gamma)
        
    return gammalist

def evaluate_gamma(gamma, mid_prices_period, buy_trades_period, sell_trades_period, k, sigma, H):
    """Run backtest for a single gamma value and return results."""
    res = run_backtest(mid_prices_period, buy_trades_period, sell_trades_period, gamma, k, sigma, H)
    final_pnl = res['pnl'][-1]
    
    if np.isnan(final_pnl) or final_pnl == 0:
        return [round(gamma, 5), np.nan, np.nan]

    spread_base = gamma * sigma**2.0 * 0.5 + (2.0 / gamma) * np.log(1.0 + (gamma / k))
    return [round(gamma, 5), final_pnl, spread_base]

def generate_gamma_grid(s, sigma, k, H):
    """Generate a grid of gamma values to test."""
    time_remaining = (H / 2.0) / 24.0
    
    def spread(gamma):
        return (gamma * sigma**2 * time_remaining + (2.0 / gamma) * np.log(1.0 + (gamma / k))) / s * 100.0
    
    try:
        gamma_001 = find_gamma(0.01, spread, k)
    except ValueError:
        _, gamma_001 = find_workable_spread(0.01, spread, k, 'up')
        if gamma_001 is None: return None
    
    try:
        gamma_1 = find_gamma(2.0, spread, k)
    except ValueError:
        _, gamma_1 = find_workable_spread(2.0, spread, k, 'down')
        if gamma_1 is None: return None
        
    return np.logspace(np.log10(gamma_1 * 0.99), np.log10(gamma_001 * 1.01), 32)

def find_gamma(target_spread, spread_func, k):
    """Find gamma for a given target spread."""
    # ... (implementation from previous steps, unchanged)
    def equation(gamma):
        if gamma <= 0: return float('inf')
        try: return spread_func(gamma) - target_spread
        except: return float('inf')
    
    def is_valid(gamma, tolerance=1e-6):
        if gamma <= 0: return False
        try: return abs(spread_func(gamma) - target_spread) < tolerance
        except: return False

    try:
        gamma_min, gamma_max = 1e-8, 1000.0
        if equation(gamma_min) * equation(gamma_max) < 0:
            gamma = brentq(equation, gamma_min, gamma_max)
            if is_valid(gamma): return gamma
    except: pass

    for guess in [1.0, k, 0.1, 10.0, k*10, k*0.1]:
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                result = fsolve(equation, guess, full_output=True)
                if result[2] == 1 and is_valid(result[0][0]): return result[0][0]
        except: continue
    
    raise ValueError(f"Could not find gamma for target_spread = {target_spread}")

def find_workable_spread(initial_spread, spread_func, k, direction='up', factor=1.05, max_iterations=100):
    """Find a workable spread if the target is not achievable."""
    # ... (implementation from previous steps, unchanged)
    spread = initial_spread
    for i in range(max_iterations):
        try:
            gamma = find_gamma(spread, spread_func, k)
            return spread, gamma
        except ValueError:
            spread *= factor if direction == 'up' else (1/factor)
    return None, None

@jit(nopython=True)
def jit_backtest_loop(s_values, buy_min_values, sell_max_values, gamma, k, sigma, fee, time_remaining, spread_base, half_spread):
    """Core JIT-compiled backtest loop."""
    # ... (implementation from previous steps, unchanged)
    N = len(s_values)
    q, x, pnl, spr, r, r_a, r_b = np.zeros(N + 1), np.zeros(N + 1), np.zeros(N + 1), np.zeros(N + 1), np.zeros(N + 1), np.zeros(N + 1), np.zeros(N + 1)
    gamma_sigma2 = gamma * sigma**2
    for i in range(N):
        r[i] = s_values[i] - q[i] * gamma_sigma2 * time_remaining[i]
        spr[i] = spread_base[i]
        gap = abs(r[i] - s_values[i])
        if r[i] >= s_values[i]:
            delta_a, delta_b = half_spread[i] + gap, half_spread[i] - gap
        else:
            delta_a, delta_b = half_spread[i] - gap, half_spread[i] + gap
        r_a[i], r_b[i] = r[i] + delta_a, r[i] - delta_b
        sell = 1 if not np.isnan(sell_max_values[i]) and sell_max_values[i] >= r_a[i] else 0
        buy = 1 if not np.isnan(buy_min_values[i]) and buy_min_values[i] <= r_b[i] else 0
        q[i+1] = q[i] + (sell - buy)
        sell_net = (r_a[i] * (1 - fee)) if sell else 0
        buy_total = (r_b[i] * (1 + fee)) if buy else 0
        x[i+1] = x[i] + sell_net - buy_total
        pnl[i+1] = x[i+1] + q[i+1] * s_values[i]
    return pnl, x, q, spr, r, r_a, r_b

def run_backtest(mid_prices, buy_trades, sell_trades, gamma, k, sigma, H, fee=0.00030):
    """Simulate the market making strategy."""
    # ... (implementation from previous steps, unchanged)
    time_index = mid_prices.index
    buy_trades_clean = buy_trades.groupby(level=0).min()
    sell_trades_clean = sell_trades.groupby(level=0).max()
    buy_min = buy_trades_clean['price'].resample('5s').min().reindex(time_index, method='ffill')
    sell_max = sell_trades_clean['price'].resample('5s').max().reindex(time_index, method='ffill')
    mid_prices = mid_prices.resample('5s').first().reindex(time_index, method='ffill')
    N = len(time_index)
    T = H / 24.0
    dt = T / N
    s_values = mid_prices.values
    buy_min_values = buy_min.values
    sell_max_values = sell_max.values
    time_remaining = T - np.arange(len(s_values)) * dt
    spread_base = gamma * sigma**2.0 * time_remaining + (2.0 / gamma) * np.log(1.0 + (gamma / k))
    half_spread = spread_base / 2.0
    pnl, x, q, spr, r, r_a, r_b = jit_backtest_loop(s_values, buy_min_values, sell_max_values, gamma, k, sigma, fee, time_remaining, spread_base, half_spread)
    return {'pnl': pnl, 'x': x, 'q': q, 'spread': spr, 'r': r, 'r_a': r_a, 'r_b': r_b}

def calculate_final_quotes(gamma, sigma, A, k, H, mid_price_df, ma_window):
    """Calculate the final reservation price and quotes."""
    print("\n" + "-"*20)
    print("Calculating final parameters for current state...")
    
    s = mid_price_df.loc[:, 'mid_price'].iloc[-1]
    time_remaining = H / 24.0
    q = 1.0  # Placeholder for current inventory

    spread_base = gamma * sigma**2.0 * time_remaining + (2.0 / gamma) * np.log(1.0 + (gamma / k))
    half_spread = spread_base / 2.0
    r = s - q * gamma * sigma**2.0 * time_remaining
    gap = abs(r - s)

    if r >= s:
        delta_a, delta_b = half_spread + gap, half_spread - gap
    else:
        delta_a, delta_b = half_spread - gap, half_spread + gap
        
    r_a, r_b = r + delta_a, r - delta_b
    
    return {
        "ticker": TICKER,
        "timestamp": pd.Timestamp.now().isoformat(),
        "market_data": {"mid_price": float(s), "sigma": float(sigma), "A": float(A), "k": float(k)},
        "optimal_parameters": {"gamma": float(gamma)},
        "current_state": {"time_remaining": float(time_remaining), "inventory": int(q), "hours_window": H, "ma_window": ma_window},
        "calculated_values": {"reservation_price": float(r), "gap": float(gap), "spread_base": float(spread_base), "half_spread": float(half_spread)},
        "limit_orders": {"ask_price": float(r_a), "bid_price": float(r_b), "delta_a": float(delta_a), "delta_b": float(delta_b),
                         "delta_a_percent": (delta_a / s) * 100.0, "delta_b_percent": (delta_b / s) * 100.0}
    }

def print_summary(results, list_of_periods):
    """Print a summary of the results to the terminal."""
    if not results:
        print("\n" + "="*80)
        print("AVELLANEDA-STOIKOV MARKET MAKING PARAMETERS")
        print("="*80)
        print("⚠️  DATA WARNING: Insufficient data for robust parameter estimation.")
        print("="*80)
        return

    TICKER = results['ticker']
    H = results['current_state']['hours_window']
    ma_window = results['current_state']['ma_window']
    
    print("\n" + "="*80)
    print(f"AVELLANEDA-STOIKOV MARKET MAKING PARAMETERS - {TICKER}")
    print(f"Analysis Period: {H} hours")
    if ma_window > 1:
        print(f"Moving Average Window: {ma_window} periods")
    print("="*80)

    if len(list_of_periods) <= 1:
        print("⚠️  DATA WARNING: Insufficient data for robust parameter estimation.")
        print("="*80)

    print(f"Market Data:")
    print(f"   Mid Price:                        ${results['market_data']['mid_price']:,.4f}")
    print(f"   Volatility (sigma):               {results['market_data']['sigma']:.6f}")
    print(f"   Intensity (A):                    {results['market_data']['A']:.4f}")
    print(f"   Order arrival rate decay (k):     {results['market_data']['k']:.6f}")
    print(f"\nOptimal Parameters:")
    print(f"   Risk Aversion (gamma): {results['optimal_parameters']['gamma']:.6f}")
    print(f"\nCurrent State:")
    print(f"   Time Remaining:        {results['current_state']['time_remaining']:.4f} (in days)")
    print(f"   Inventory (q):         {results['current_state']['inventory']:.4f}")
    print(f"\nCalculated Prices:")
    print(f"   Reservation Price:     ${results['calculated_values']['reservation_price']:.4f}")
    print(f"   Ask Price:             ${results['limit_orders']['ask_price']:.4f}")
    print(f"   Bid Price:             ${results['limit_orders']['bid_price']:.4f}")
    print(f"\nSpreads:")
    print(f"   Delta Ask:             ${results['limit_orders']['delta_a']:.6f} ({results['limit_orders']['delta_a_percent']:.6f}%)")
    print(f"   Delta Bid:             ${results['limit_orders']['delta_b']:.6f} ({results['limit_orders']['delta_b_percent']:.6f}%)")
    print(f"   Total Spread:          {(results['limit_orders']['delta_a_percent'] + results['limit_orders']['delta_b_percent']):.4f}%")
    
    json_filename = f"avellaneda_parameters_{TICKER}.json"
    with open(json_filename, 'w') as f:
        json.dump(results, f, indent=4)
    print(f"\nResults saved to: {json_filename}")
    print("="*80)

def main():
    """Main execution function."""
    global TICKER  # Make TICKER a global variable
    args = parse_arguments()
    TICKER = args.ticker
    H = args.hours
    
    if H <= 8: ma_window = 3
    elif 8 < H < 20: ma_window = 2
    else: ma_window = 1

    print("-" * 20)
    print(f"DOING: {TICKER}")
    print(f"Using analysis period of {H} hours.")
    if ma_window > 1:
        print(f"Using a {ma_window}-period moving average for parameters.")

    tick_size = get_tick_size(TICKER)
    delta_list = np.arange(tick_size, 50.0 * tick_size, tick_size)
    
    # Load data
    script_dir = Path(__file__).parent.absolute()
    default_if_not_env = script_dir / 'edgex_data'
    HL_DATA_DIR = os.getenv('HL_DATA_LOC', default_if_not_env)
    csv_file_path = os.path.join(HL_DATA_DIR, f'tickers_{TICKER}.csv')

    if not os.path.exists(csv_file_path):
        print(f"Error: File {csv_file_path} not found!")
        sys.exit(1)

    mid_price_df = load_and_resample_mid_price(csv_file_path)
    trades_df = load_trades_data(os.path.join(HL_DATA_DIR, f'trades_{TICKER}.csv'))
    buy_trades = trades_df[trades_df['side'] == 'buy'].copy()
    sell_trades = trades_df[trades_df['side'] == 'sell'].copy()
    print(f"Loaded {len(mid_price_df)} data points from {mid_price_df.index.min()} to {mid_price_df.index.max()}.")

    freq_str = f'{H}h'
    list_of_periods = mid_price_df.index.floor(freq_str).unique().tolist()[:-1]

    # Calculate parameters
    sigma_list = calculate_volatility(mid_price_df, H, freq_str)
    Alist, klist = calculate_intensity_params(list_of_periods, H, buy_trades, sell_trades, delta_list, mid_price_df)
    
    if len(list_of_periods) <= 1:
        print_summary({}, list_of_periods)
        sys.exit()

    gammalist = optimize_gamma(list_of_periods, sigma_list, Alist, klist, H, ma_window, mid_price_df, buy_trades, sell_trades, tick_size)

    # Final calculations
    if len(gammalist) > 0:
        if ma_window > 1:
            gamma_slice = gammalist[max(0, len(gammalist) - ma_window):]
            gamma = pd.Series(gamma_slice).mean()
        else:
            gamma = gammalist[-1]
        if pd.isna(gamma): gamma = 0.1
    else:
        gamma = 0.1

    if ma_window > 1:
        start_index = max(0, len(Alist) - 1 - ma_window + 1)
        end_index = len(Alist) - 1
        a_slice = Alist[start_index:end_index]
        k_slice = klist[start_index:end_index]
        A = pd.Series(a_slice).mean()
        k = pd.Series(k_slice).mean()
    else:
        A = Alist[-2] if len(Alist) > 1 else Alist[-1]
        k = klist[-2] if len(klist) > 1 else klist[-1]

    if pd.isna(A): A = Alist[-2] if len(Alist) > 1 else Alist[-1]
    if pd.isna(k): k = klist[-2] if len(klist) > 1 else klist[-1]
    
    sigma = sigma_list[-2] if len(sigma_list) > 1 else sigma_list[-1]

    results = calculate_final_quotes(gamma, sigma, A, k, H, mid_price_df, ma_window)
    print_summary(results, list_of_periods)

if __name__ == "__main__":
    main()