# GVRET_to_mf4

This repository provides a Python script for converting CAN bus GVRET CSV log files (SavvyCAN) to MF4 format (compatible with asammdf).

## Usage
Clone and install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Example (as a module)
> **Note:** SavvyCAN uses nanoseconds (`ns`) by default for timestamps. If your GVRET file is from SavvyCAN, set `time_unit="ns"` (the default).
```python
from gvret_to_mf4 import convert_gvret_to_mf4

convert_gvret_to_mf4(
   input_file="input.gvret",
   output_file="output.mf4",
   dbc_path="dbc_file.dbc",
   time_unit="ns"  # or "s", "ms", "us" as appropriate
)
```

## License
See [LICENSE](LICENSE.txt) for details.
