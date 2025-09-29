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

from asammdf import MDF, Signal
import pandas as pd
import numpy as np
import cantools

def convert_gvret_to_mf4(input_file, output_file, dbc_path, time_unit="ns"):
    """
    Convert a GVRET log file to MF4 format using a DBC file.
    - input_file: path to the GVRET CSV log file
    - output_file: path to save the MF4 file
    - dbc_path: path to the DBC file for CAN decoding
    - time_unit: unit of the time column ('s', 'ms', 'us', 'ns').
        Note: SavvyCAN uses nanoseconds ('ns') by default for timestamps.
    """
    db = cantools.database.load_file(dbc_path)
    mdf = MDF()
    # Data types for each column in the GVRET log file
    data_types = {"Time Stamp": np.uint64, "ID": str, "Extended": bool, "Dir": str, "Bus": int, "LEN": int, "D1": str, "D2": str, "D3": str, "D4": str, "D5": str, "D6": str, "D7": str, "D8": str}

    # Convert D1-D8 columns into a single byte array per row
    def bytes_to_array(row):
        hex_string = ''.join([row[col] for col in ['D1', 'D2', 'D3', 'D4', 'D5', 'D6', 'D7', 'D8']])
        return bytes.fromhex(hex_string)

    df = pd.read_csv(input_file, dtype=data_types, index_col=False)
    start_time = df["Time Stamp"].min()

    # Convert "Time Stamp" to seconds (MDF uses seconds) using pandas to_timedelta for robust handling
    valid_units = {"s", "ms", "us", "ns"}
    pandas_unit = time_unit.lower()
    if pandas_unit not in valid_units:
        raise ValueError(f"Unsupported time_unit: {time_unit}. Choose from 's', 'ms', 'us', 'ns'.")
    df["Time Stamp"] = pd.to_timedelta(df["Time Stamp"] - start_time, unit=pandas_unit).total_seconds()

    # Ensure ID is in integer format (assuming hexadecimal)
    df["ID"] = df["ID"].apply(lambda x: int(x, 16))

    df["Data"] = df.apply(bytes_to_array, axis=1)
    data = {}

    # Add channels to the MDF file
    for _, row in df.iterrows():
        # turn off decode_choices to avoid decoding enumerations into strings since mf4 only supports numbers
        message = db.decode_message(row["ID"], bytes(row["Data"]), decode_choices=False)
        for signal in message:
            if str(signal) not in data:
                data[str(signal)] = ([], [])
            data[str(signal)][0].append(row["Time Stamp"])
            data[str(signal)][1].append(message[signal])

    for key, value in data.items():
        sig = Signal(samples=value[1], timestamps=value[0], name=key, encoding='utf-8', unit='')
        mdf.append(sig)
    mdf.save(output_file)
