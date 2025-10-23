
import logging
from typing import Any
import pandas as pd
import numpy as np
from asammdf import MDF, Signal
import cantools

import logging

'''
TODO:
- re-test this file to make sure it still works
- try importing this as submodule into firmware project
- set output folder as a parameter OR can this be set using output file with relative pathing?
- try both python import and command line run

> turn off decode_choices to avoid decoding enumerations into strings since mf4 only supports numbers
- this is bad, can we fix this?
'''

def convert_gvret_to_mf4(
    input_file: str,
    output_file: str,
    dbc_path: str,
    time_unit: str = "ns"
) -> None:
    """
    Convert a GVRET log file to MF4 format using a DBC file.

    Args:
        input_file (str): Path to the GVRET CSV log file.
        output_file (str): Path to save the MF4 file.
        dbc_path (str): Path to the DBC file for CAN decoding.
        time_unit (str, optional): Unit of the time column ('s', 'ms', 'us', 'ns'). Defaults to 'ns'.

    Raises:
        ValueError: If the time_unit is not supported.
        FileNotFoundError: If input_file or dbc_path does not exist.
        Exception: For other errors during conversion.
    """

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    # Validate input files
    import os
    if not os.path.isfile(input_file):
        logging.error(f"Input file not found: {input_file}")
        raise FileNotFoundError(f"Input file not found: {input_file}")
    if not os.path.isfile(dbc_path):
        logging.error(f"DBC file not found: {dbc_path}")
        raise FileNotFoundError(f"DBC file not found: {dbc_path}")

    # Data types for each column in the GVRET log file
    data_types = {
        "Time Stamp": np.uint64, "ID": str, "Extended": bool, "Dir": str, "Bus": int, "LEN": int,
        "D1": str, "D2": str, "D3": str, "D4": str, "D5": str, "D6": str, "D7": str, "D8": str
    }

    # Vectorized conversion of D1-D8 columns to bytes
    hex_cols = [f'D{i}' for i in range(1, 9)]
    df['DataHex'] = df[hex_cols].agg(''.join, axis=1)
    try:
        df['Data'] = df['DataHex'].apply(bytes.fromhex)
    except Exception as e:
        logging.error(f"Failed to convert data bytes for some rows: {e}")
        raise

    try:
        db = cantools.database.load_file(dbc_path)
    except Exception as e:
        logging.error(f"Failed to load DBC file: {e}")
        raise

    try:
        df = pd.read_csv(input_file, dtype=data_types, index_col=False)
    except Exception as e:
        logging.error(f"Failed to read input CSV: {e}")
        raise

    if "Time Stamp" not in df.columns or "ID" not in df.columns:
        logging.error("Input CSV missing required columns: 'Time Stamp' and/or 'ID'")
        raise ValueError("Input CSV missing required columns: 'Time Stamp' and/or 'ID'")

    # --- Robust time and ID conversion ---
    start_time = df["Time Stamp"].min()
    valid_units = {"s", "ms", "us", "ns"}
    pandas_unit = time_unit.lower()
    if pandas_unit not in valid_units:
        raise ValueError(f"Unsupported time_unit: {time_unit}. Choose from 's', 'ms', 'us', 'ns'.")
    try:
        df["Time Stamp"] = pd.to_timedelta(df["Time Stamp"] - start_time, unit=pandas_unit).dt.total_seconds()
    except Exception as e:
        logging.error(f"Failed to convert time units: {e}")
        raise

    def parse_id(x: str) -> int:
        try:
            # Ensure ID is in integer format (assuming hexadecimal)
            return int(x, 16)
        except Exception as e:
            logging.error(f"Failed to parse CAN ID '{x}': {e}")
            raise
    try:
        df["ID"] = df["ID"].apply(parse_id)
    except Exception as e:
        logging.error(f"Failed to convert CAN IDs: {e}")
        raise


    mdf = MDF()
    data = {}
    total_rows = len(df)

    # Use itertuples(name=None) and access columns by index for robustness
    col_idx = {col: i for i, col in enumerate(df.columns)}
    idx_time = col_idx['Time Stamp']
    idx_id = col_idx['ID']
    idx_data = col_idx['Data']

    for idx, row in enumerate(df.itertuples(index=False, name=None), 1):
        if idx % 5000 == 0 or idx == total_rows:
            logging.info(f"Processed row {idx} of {total_rows}")
        try:
            # Turn off decode_choices to avoid decoding enumerations into strings since mf4 only supports numbers
            message = db.decode_message(row[idx_id], bytes(row[idx_data]), decode_choices=False)
        except KeyError:
            logging.warning(f"CAN ID {row[idx_id]} not found in DBC. Skipping row {idx}.")
            continue
        except Exception as e:
            logging.error(f"Failed to decode message at row {idx}: {e}")
            continue

        timestamp = row[idx_time]
        if timestamp is None or pd.isna(timestamp):
            logging.warning(f"Row {idx} has invalid timestamp. Skipping.")
            continue
        for signal, value in message.items():
            if signal not in data:
                data[signal] = ([], [])  # (timestamps, samples)
            data[signal][0].append(timestamp)
            data[signal][1].append(value)


    logging.info(f"File {input_file} converted successfully, saving to MDF4...")
    for key, value in data.items():
        timestamps, samples = value[0], value[1]
        # Sort by timestamp and remove duplicates or out-of-order
        sorted_pairs = sorted(zip(timestamps, samples))
        sorted_timestamps = []
        sorted_samples = []
        last_ts = None
        for ts, s in sorted_pairs:
            if last_ts is None or ts > last_ts:
                sorted_timestamps.append(ts)
                sorted_samples.append(s)
                last_ts = ts
            # else: skip duplicate or out-of-order timestamp
        sig = Signal(samples=sorted_samples, timestamps=sorted_timestamps, name=key, encoding='utf-8', unit='')
        mdf.append(sig)

    try:
        mdf.save(output_file)
        logging.info(f"Saved to {output_file}")
    except Exception as e:
        logging.error(f"Failed to save MDF file: {e}")
        raise
