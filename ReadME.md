## Overview
In the `Part_2` directory, I used a script named `week_5_lab.py` to process the original `YelloTaxiData.csv` file. The purpose of this script was to reduce the number of rows in the dataset to 100 rows. This was because the original file was too large to be sent as an attachment for testing the cloud flow.

## Process
1. **Original File**: `YelloTaxiData.csv` (not included in the repository)
2. **Script Used**: `week_5_lab.py`
3. **Operation**: Reduced the rows to 100
4. **New File**: `YelloTaxiData_new.csv` (not included in the repository)

## Purpose
The reduced file size ensures that the dataset can be easily attached and sent for testing the cloud flow without any issues related to file size limitations.

## Directory Structure
```
Part_2/
│
├── week_5_lab.py
├── YelloTaxiData.csv (original, gitignored)
└── YelloTaxiData_new.csv (reduced to 100 rows, gitignored)
```
## Conclusion
By reducing the size of the `YelloTaxiData.csv` file, it is now possible to test the cloud flow. The `week_5_lab.py` in the `Part_2` directory handles this reduction process. The `.csv` files are gitignored and not included in the repository due to their size.