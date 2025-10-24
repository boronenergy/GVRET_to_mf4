# Copyright 2025 Boron Energy Corp.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
import pandas as pd
import numpy as np
import binascii
from asammdf import MDF, Signal
import cantools
from concurrent.futures import ThreadPoolExecutor

_INITIAL_ERROR_LOG_COUNT = 10
_ERROR_LOG_INTERVAL = 10000

def convert_gvret_to_mf4(
    input_file: str,
    output_file: str,
    dbc_path: str,
    time_unit: str = "us"
) -> None:
    """
    Convert a GVRET log file to MF4 format using a DBC file.

    Args:
        input_file (str): Path to the GVRET CSV log file.
        output_file (str): Path to save the MF4 file.
        dbc_path (str): Path to the DBC file for CAN decoding.
        time_unit (str, optional): Unit of the time column ('s', 'ms', 'us'). Defaults to 'us'.

    Raises:
        ValueError: If the time_unit is not supported.
        FileNotFoundError: If input_file or dbc_path does not exist.
        Exception: For other errors during conversion.
    """

    # Validate input files
    if not os.path.isfile(input_file):
        logging.error(f"Input file not found: {input_file}")
        raise FileNotFoundError(f"Input file not found: {input_file}")
    if not os.path.isfile(dbc_path):
        logging.error(f"DBC file not found: {dbc_path}")
        raise FileNotFoundError(f"DBC file not found: {dbc_path}")

    # Only read needed columns
    needed_cols = ["Time Stamp", "ID", "Extended", "Dir", "Bus", "LEN"] + [f"D{i}" for i in range(1, 9)]
    # Optimize dtypes: use category for repeated strings, smallest ints for numerics
    data_types = {
        "Time Stamp": np.uint64,
        "ID": "category",
        "Extended": bool,
        "Dir": "category",
        "Bus": np.uint8,
        "LEN": np.uint8,
        **{f"D{i}": str for i in range(1, 9)}
    }

    logging.info(f"Reading input CSV: {input_file}")
    try:
        df = pd.read_csv(
            input_file,
            dtype=data_types,
            usecols=needed_cols,
            index_col=False,
            low_memory=False,
            engine='c',
            memory_map=True
        )
    except Exception as e:
        logging.error(f"Failed to read input CSV: {e}")
        raise

    # Vectorized conversion of D1-D8 columns to bytes (must be after reading CSV)
    hex_cols = [f'D{i}' for i in range(1, 9)]
    df[hex_cols] = df[hex_cols].fillna('')
    df['DataHex'] = np.char.add.reduce([df[col].values for col in hex_cols])
    try:
        df['Data'] = df['DataHex'].str.lower().map(binascii.unhexlify)
    except Exception as e:
        logging.error(f"Failed to convert data bytes for some rows: {e}")
        raise
    df.drop(columns=['DataHex'], inplace=True)

    try:
        db = cantools.database.load_file(dbc_path)
    except Exception as e:
        logging.error(f"Failed to load DBC file: {e}")
        raise

    if "Time Stamp" not in df.columns or "ID" not in df.columns:
        logging.error("Input CSV missing required columns: 'Time Stamp' and/or 'ID'")
        raise ValueError("Input CSV missing required columns: 'Time Stamp' and/or 'ID'")

    # --- Robust time and ID conversion ---
    start_time = df["Time Stamp"].min()
    valid_units = {"s", "ms", "us"}
    pandas_unit = time_unit.lower()
    if pandas_unit not in valid_units:
        raise ValueError(f"Unsupported time_unit: {time_unit}. Choose from 's', 'ms', 'us'.")
    try:
        df["Time Stamp"] = pd.to_timedelta(df["Time Stamp"] - start_time, unit=pandas_unit).dt.total_seconds()
    except Exception as e:
        logging.error(f"Failed to convert time units: {e}")
        raise

    try:
        # Ensure ID is in integer format (assuming hexadecimal)
        df["ID"] = df["ID"].astype(str).apply(lambda x: int(x, 16))
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

    # Only log at start/end and on error for speed
    logging.info(f"Starting CAN message decoding for {total_rows} rows...")
    for idx, row in enumerate(df.itertuples(index=False, name=None), 1):
        try:
            # Turn off decode_choices to avoid decoding enumerations into strings since mf4 only supports numbers
            message = db.decode_message(row[idx_id], bytes(row[idx_data]), decode_choices=False)
        except KeyError:
            logging.warning(f"Unknown CAN ID {row[idx_id]} at row {idx}, skipping")
            continue
        except Exception as e:
            if idx < _INITIAL_ERROR_LOG_COUNT or idx % _ERROR_LOG_INTERVAL == 0:
                logging.error(f"Failed to decode message at row {idx}: {e}")
            continue
        timestamp = row[idx_time]
        if timestamp is None or pd.isna(timestamp):
            logging.warning(f"Invalid timestamp at row {idx}, skipping")
            continue
        for signal, value in message.items():
            if signal not in data:
                data[signal] = ([], [])  # (timestamps, samples)
            data[signal][0].append(timestamp)
            data[signal][1].append(value)

    # Sort to ensure that each signal’s samples are in strictly increasing timestamp order in the MF4 file
    logging.info(f"File {input_file} converted successfully, sorting")
    def sort_and_create_signal(args):
        key, value = args
        timestamps, samples = value[0], value[1]
        timestamps = np.array(timestamps)
        samples = np.array(samples)
        if len(timestamps) == 0:
            return None
        order = np.argsort(timestamps)
        sorted_timestamps = timestamps[order]
        sorted_samples = samples[order]
        # Vectorized strictly increasing filter
        mask = np.concatenate(([True], sorted_timestamps[1:] > sorted_timestamps[:-1]))
        if not np.any(mask):
            return None
        sig = Signal(
            samples=sorted_samples[mask],
            timestamps=sorted_timestamps[mask],
            name=key,
            encoding='utf-8',
            unit=''
        )
        return sig

    with ThreadPoolExecutor() as executor:
        for sig in executor.map(sort_and_create_signal, data.items()):
            if sig is not None:
                mdf.append(sig)

    try:
        mdf.save(output_file)
        logging.info(f"Saved to {output_file}")
    except Exception as e:
        logging.error(f"Failed to save MDF file: {e}")
        raise
