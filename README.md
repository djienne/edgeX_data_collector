# EdgeX Market Data Collector

A simple and reliable Python script to collect real-time market data from the EdgeX exchange via its public WebSocket API.

This collector connects to the EdgeX WebSocket, subscribes to specified market data channels, and saves the incoming data into organized CSV files.

## Features

- **Multiple Data Streams**: Collects Ticker, Trade, and Order Book (depth) data.
- **Persistent Storage**: Saves data into separate CSV files for each symbol and data type (e.g., `trades_BTC.csv`, `depth_ETH.csv`).
- **Stateful Order Book**: Reconstructs a full order book snapshot for each update, providing a clean, per-update view of the top 15 levels.
- **Robust Connection**: Automatically handles reconnection and subscription in case of network issues.
- **Dockerized**: Includes `docker-compose.yml` for easy, cross-platform deployment.

## How to Run

### Using Docker (Recommended)

This is the easiest way to run the collector.

**Prerequisites**: Docker and Docker Compose must be installed.

1.  Open a terminal in the project directory.
2.  Run the following command to build and start the collector in the background:

    ```sh
    docker-compose up -d
    ```

Data will be saved to the `edgex_data` directory.

### Using Python Directly

**Prerequisites**: Python 3.10+.

1.  Install the required dependencies:

    ```sh
    pip install -r requirements.txt
    ```

2.  Run the script:

    ```sh
    python edgex_data_collector.py
    ```

## Configuration

The crypto contracts to monitor can be adjusted by modifying the `CONTRACT_MAP` dictionary within the `edgex_data_collector.py` script.

### `calculate_avellaneda_parameters.py`

This script analyzes the collected market data (prices and trades) to estimate the key parameters for the Avellaneda-Stoikov market making model.

- **Volatility (σ)**: Measures the standard deviation of log-returns.
- **Order Arrival Intensity (A, k)**: Models the rate of order arrivals at different price levels.
- **Risk Aversion (γ)**: Optimizes the trade-off between inventory risk and spread profit via backtesting.

It outputs the calculated parameters, reservation price, and optimal bid/ask quotes.

### `avellaneda_parameters_PAXG.json`

A sample output file containing the calculated parameters for PAXG. This file includes:
- **Market Data**: Mid-price, volatility (sigma), and intensity parameters (A, k).
- **Optimal Parameters**: The calculated risk aversion parameter (gamma).
- **Calculated Prices**: The reservation price and optimal bid/ask limit orders based on the model.
