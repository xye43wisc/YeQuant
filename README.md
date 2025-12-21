# YeQuant - A-Share Quantitative Analysis System

A quantitative trading analysis system built with Python, designed for high performance and extensibility.

## Features
- **Data Acquisition**: Stable data downloading using proprietary `AmazingData` SDK.
- **Data Storage**: Efficient storage using SQLite.
- **Analysis**: Pandas-based data processing and analysis.

## Prerequisites
- Python 3.11+
- `AmazingData` SDK (Proprietary wheel)
- `tgw` SDK (Proprietary wheel)

## Installation

1. **Clone the repository** (or download source):
   ```bash
   git clone <your-repo-url>
   cd YeQuant
   ```

2. **Set up Virtual Environment**:
   ```bash
   python -m venv .YeQuantVenv
   # Windows:
   .YeQuantVenv\Scripts\activate
   # Linux/Mac:
   source .YeQuantVenv/bin/activate
   ```

3. **Install Dependencies**:
   
   First, install the local SDK wheels (located in the root or provided separately):
   ```bash
   pip install tgw-1.0.8.4-py3-none-any.whl
   pip install AmazingData-1.0.23-cp311-none-any.whl
   ```
   
   Then install standard requirements:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

Before running the project, you must create a `config.json` file in the root directory with your credentials.

**`config.json` format:**
```json
{
    "USER": "YOUR_ACCOUNT_ID",
    "PWD": "YOUR_PASSWORD",
    "IP": "YOUR_IP",
    "PORT": YOUR_PORT
}
```

> **Note**: `config.json` is ignored by Git to protect your sensitive information.

## Usage

Run the main analysis script:
```bash
python main.py
```
