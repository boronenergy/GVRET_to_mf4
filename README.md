# GVRET_to_mf4

This repository provides a Python script for converting CAN bus GVRET CSV log files (SavvyCAN) to MF4 format (compatible with asammdf).

## CAN Logging Workflow

- [SavvyCAN](https://github.com/collin80/SavvyCAN) is an open-source, free CAN logging software compatible with socketcan, Vector, PeakCAN, and TinyCAN interfaces.
- SavvyCAN is great for real-time logging, but lacks great UI for debugging large logs with many signals. This is where [asammdfgui](https://www.csselectronics.com/pages/asammdf-gui-api-mdf4-mf4) shines.
- This library converts SavvyCAN GVRET CSV logs to asammdfgui-compatible MF4 files.

## Usage
Clone and install dependencies:
   ```bash
   pip install -r requirements.txt
   ```


## Example (as a module)
> **Note:** Most GVRET logs use microseconds (`us`) for timestamps.
```python
from gvret_to_mf4 import convert_gvret_to_mf4

convert_gvret_to_mf4(
   input_file="input.gvret",
   output_file="output.mf4",
   dbc_path="dbc_file.dbc",
   time_unit="us"  # or "s", "ms" as appropriate
)
```

## Example (from the command line)
```bash
python -m gvret_to_mf4 input.gvret output.mf4 dbc_file.dbc --time_unit us
```

## License
See [LICENSE](LICENSE.txt) for details.
