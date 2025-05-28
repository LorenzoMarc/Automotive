# Automotive Tool Suite

This project is a comprehensive suite of tools for automotive data analysis, DBC/feature compliance, CAN log parsing, transfer function checks, DLT log parsing, and policy management. It provides a graphical user interface (GUI) for each major function, supporting workflows such as CAN log mapping, DBC feature compliance, DBC comparison, V2C log parsing, transfer function analysis, and policy loading.

## Features

- **CAN Log Mapping:** Parse CAN logs (`.asc`/`.blf`) and map them to DBC files, exporting results to CSV and uploading to a PostgreSQL database.
- **Transfer Function Check:** Analyze transfer functions against DBC files and upload results.
- **Feature Compliance:** Check feature files against DBCs for compliance, including architecture and vehicle-specific checks.
- **DBC Compare:** Compare multiple DBC files and upload comparison results.
- **V2C Log Reader:** Parse V2C logs for different regions and upload to the database.
- **Policy Loader:** Parse and upload policy files in JSON format.
- **DLT Loader:** Parse DLT logs and extract signal data.
- **Reporting:** Generate compliance reports in Excel and PDF formats.

## Requirements

- Python 3.8+
- PostgreSQL database (default connection: `localhost:5432`, user: `postgres`, password: `admin`, db: `postgres`)
- See [requirements.txt](requirements.txt) for Python dependencies.

## Installation

1. **Clone the repository:**
   ```sh
   git clone <repo-url>
   cd tool_l
   ```

2. **Install dependencies:**
   ```sh
   pip install -r requirements.txt
   ```

3. **(Optional) Build executable:**
   - You can use [PyInstaller](https://www.pyinstaller.org/) to build a standalone executable:
     ```sh
     pyinstaller.exe --onedir .\main.py --icon .\logo.png
     ```

## Usage

Launch the main GUI:
```sh
python main.py
```

You will see a window with the following options:
- **CAN log mapping on DBC**
- **Transfer Function Check on DBC**
- **DBC Feature Compliance**
- **DBC Compare**
- **V2C Logger**
- **Policy Loader**
- **DLT Loader**

Each tool provides its own interface for file selection and database upload.

### Database Connection

You can set the database connection parameters (port, db, user, password, host) in the main window. Defaults are provided if left blank.

### Output

- Processed data is uploaded to the PostgreSQL database.
- Reports are generated as `.xlsx` and `.pdf` files in the working directory.

## File Structure

- `main.py` - Main GUI entry point.
- `gui_*.py` - GUI modules for each tool.
- `*_parser.py` - Parsers for DBC, CAN, DLT, global logs, and policies.
- `feature_dbc.py`, `action1.py`, `action2.py` - Feature and transfer function processing.
- `utils_db.py`, `utils_sc.py` - Utility functions for database and schedule handling.
- `reporting.py` - Compliance report generation.
- `stored_proc.py` - Calls to PostgreSQL stored procedures.
- `requirements.txt` - Python dependencies.

## Notes

- Ensure your PostgreSQL server is running and accessible with the provided credentials.
- Some tools require specific file formats (e.g., `.dbc`, `.asc`, `.xlsx`, `.json`).
- For PDF report generation, [WeasyPrint](https://weasyprint.org/) and GTK3 runtime may be required on Windows.

## License


---

For any issues or questions, please refer to the code or contact the project maintainer.