
import os
import pandas as pd
import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timedelta
import traceback

# Load the CSV file into a pandas DataFrame
try:
    script_df = pd.read_csv('api-scrip-master.csv')
except FileNotFoundError:
    raise RuntimeError("Error: api-scrip-master.csv not found. Please make sure the file is in the same directory.")

app = FastAPI()

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Your DhanHQ Access Token - It's recommended to use an environment variable for this
# For local testing you can set it here, but for Render, use environment variables.
DHAN_ACCESS_TOKEN = os.environ.get("DHAN_ACCESS_TOKEN", "YOUR_DHAN_ACCESS_TOKEN_HERE")
print(f"--- USING DHAN_ACCESS_TOKEN ---: {DHAN_ACCESS_TOKEN}")

DHAN_API_URL = "https://api.dhan.co/v2/charts/intraday"

@app.get("/oanda/api/v1/klines")
def get_klines(symbol: str, interval: str, startTime: int = None, endTime: int = None):
    """
    Fetches historical kline data for a given symbol, interval, and optional time range.
    Mimics the Binance API endpoint.
    """
    # 1. Find the instrument details from the CSV
    instrument_details = script_df[script_df['tradingSymbol'] == symbol]

    if instrument_details.empty:
        raise HTTPException(status_code=404, detail=f"Instrument with trading symbol '{symbol}' not found.")

    instrument = instrument_details.iloc[0]
    security_id = instrument['securityId']
    exchange_segment = instrument['exchangeSegment']
    instrument_type = instrument['instrument']

    # 2. Determine the date range
    if startTime and endTime:
        from_date = datetime.fromtimestamp(startTime / 1000)
        to_date = datetime.fromtimestamp(endTime / 1000)
    else:
        to_date = datetime.now()
        from_date = to_date - timedelta(days=30)

    from_date_str = from_date.strftime('%Y-%m-%d %H:%M:%S')
    to_date_str = to_date.strftime('%Y-%m-%d %H:%M:%S')

    # 3. Parse the interval to get the integer value
    try:
        parsed_interval = ''.join(filter(str.isdigit, interval))
        if not parsed_interval:
            raise ValueError("Invalid interval format")
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail=f"Invalid interval format: '{interval}'")

    # 4. Prepare and make the request to DhanHQ API
    headers = {
        "access-token": DHAN_ACCESS_TOKEN,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    payload = {
        "securityId": str(security_id),
        "exchangeSegment": exchange_segment,
        "instrument": instrument_type,
        "interval": parsed_interval,
        "fromDate": from_date_str,
        "toDate": to_date_str
    }

    print(f"--- SENDING REQUEST TO DHANHQ ---")
    print(f"URL: {DHAN_API_URL}")
    print(f"Headers: {headers}")
    print(f"Payload: {payload}")
    try:
        response = requests.post(DHAN_API_URL, headers=headers, json=payload)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error calling DhanHQ API: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error calling DhanHQ API: {e}")

    try:
        dhan_data = response.json()
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON response from DhanHQ API: {e}")
        print(f"Response content: {response.text}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error decoding JSON response from DhanHQ API: {e}")

    # 5. Transform the data to Binance kline format
    try:
        # Combine the lists into a list of tuples
        zipped_data = zip(
            dhan_data['timestamp'],
            dhan_data['open'],
            dhan_data['high'],
            dhan_data['low'],
            dhan_data['close'],
            dhan_data['volume']
        )

        # Format into a list of lists, converting types as needed
        binance_format_data = [
            [
                int(ts) * 1000,      # Timestamp to milliseconds
                str(o),              # Open to string
                str(h),              # High to string
                str(l),              # Low to string
                str(c),              # Close to string
                str(v)               # Volume to string
            ]
            for ts, o, h, l, c, v in zipped_data
        ]
        return binance_format_data
    except (KeyError, TypeError) as e:
        print(f"Error transforming DhanHQ data: {e}")
        print(f"DhanHQ Data: {dhan_data}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Failed to parse DhanHQ response: {e}")

@app.get("/")
def read_root():
    return {"message": "DhanHQ Historical Data API Proxy is running."}

# To run this application locally:
# 1. Make sure your virtual environment is activated: venv\Scripts\activate
# 2. Set your access token: set DHAN_ACCESS_TOKEN=your_real_token
# 3. Run the server: uvicorn main:app --reload
