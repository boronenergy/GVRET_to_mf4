import argparse
import logging
from .core import convert_gvret_to_mf4

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Convert GVRET log files to MF4 format using a DBC file.")
    parser.add_argument("input_file", help="Path to the GVRET CSV log file")
    parser.add_argument("output_file", help="Path to save the MF4 file")
    parser.add_argument("dbc_path", help="Path to the DBC file for CAN decoding")
    parser.add_argument("--time_unit", default="us", choices=["s", "ms", "us"], help="Unit of the time column: s, ms, us (default: us)")
    args = parser.parse_args()
    convert_gvret_to_mf4(
        input_file=args.input_file,
        output_file=args.output_file,
        dbc_path=args.dbc_path,
        time_unit=args.time_unit
    )
