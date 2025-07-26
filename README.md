# PowerBI Data Export Tool

A robust Python application for automated data extraction from PowerBI datasets with 12-month rolling window support, intelligent retry mechanisms, and multiple export formats.

## 🚀 Features

- **12-Month Rolling Extraction**: Automatically extracts data for the past 12 months
- **Multiple Export Formats**: Supports both CSV and Parquet formats
- **Intelligent Retry System**: Automatically retries failed extractions with smart error handling
- **Session Management**: Handles PowerBI session timeouts and connection issues
- **Progress Tracking**: Real-time progress bars with Rich console interface
- **State Persistence**: Remembers failed extractions between runs
- **Modular Architecture**: Clean, maintainable code structure
- **Portable Executable**: Can be compiled to a standalone .exe file

## 📋 Prerequisites

- Python 3.8 or higher
- .NET Framework 4.5+ (for ADOMD.NET client)
- PowerBI Pro or Premium license
- Access to PowerBI workspace and dataset

## 🛠️ Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/darkseidH/automate-powerbi-export.git
   cd automate-powerbi-export
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Install Python.NET for PowerBI connectivity**:
   ```bash
   pip install pythonnet
   ```

## ⚙️ Configuration

1. **Copy the environment template**:
   ```bash
   cp .env.example .env
   ```

2. **Edit `.env` file with your PowerBI credentials**:
   ```env
   POWERBI_SERVER=powerbi://api.powerbi.com/v1.0/myorg/YOUR_WORKSPACE_NAME
   POWERBI_DATABASE=YOUR_DATASET_NAME
   
   # Optional: Adjust timeouts if needed
   CONNECT_TIMEOUT=30
   COMMAND_TIMEOUT=600
   ```

3. **Ensure your DAX query is in place**:
   - Place your query template in `queries/billing_cases_query.dax`
   - Use placeholders: `{year}`, `{month}`, `{day_start}`, `{day_end}`

## 🚦 Usage

### Running from Source

```bash
python main.py
```

### Running as Executable

```bash
# Build the executable
python build_exe.py

# Run the executable
dist/PowerBIExport_Distribution/PowerBIExport.exe
```

### Customizing Date Range

By default, the tool extracts 12 months ending with the current month. To specify a custom endpoint, modify `main.py`:

```python
# In main() function
pipeline.run(end_year=2025, end_month=7)  # 12 months ending July 2025
```

## 📁 Project Structure

```
powerbi_export/
├── main.py                    # Entry point & orchestration
├── config/
│   └── settings.py           # Configuration management
├── core/
│   ├── connection.py         # PowerBI connection handling
│   ├── query_executor.py     # DAX query execution
│   └── data_processor.py     # Data conversion utilities
├── exporters/
│   ├── base.py              # Abstract exporter interface
│   ├── csv_exporter.py      # CSV export implementation
│   └── parquet_exporter.py  # Parquet export implementation
├── utils/
│   ├── date_manager.py      # Date range calculations
│   ├── progress_tracker.py  # Progress bar utilities
│   ├── retry_manager.py     # Retry logic & error handling
│   ├── runtime.py           # Runtime path resolution
│   └── state_manager.py     # State persistence
├── logger/
│   └── logger.py            # Logging configuration
├── lib/                     # ADOMD.NET assemblies
├── queries/                 # DAX query templates
└── exported_data/           # Output directory
    ├── csv/                 # CSV exports
    └── parquet/            # Parquet exports
```

## 🔄 Retry Mechanism

The tool automatically handles common PowerBI errors:

- **Session Expired**: Creates new connection immediately
- **Connection Timeout**: Increases timeout values progressively
- **Memory Errors**: Clears memory and waits before retry
- **Maximum 5 retry attempts** per month
- Failed months are saved to `retry_state.json` for persistence

## 📊 Output Files

Exported files follow this naming convention:
```
billing_cases_YYYY_MM_DD_DD.csv
billing_cases_YYYY_MM_DD_DD.parquet
```

Example: `billing_cases_2025_07_01_31.csv` for July 2025 data

## 🐛 Troubleshooting

### Common Issues

1. **"Session ID cannot be found" Error**
   - The tool will automatically retry with a new connection
   - If persistent, check PowerBI workspace permissions

2. **Connection Timeout**
   - Increase timeout values in `.env` file
   - Check network connectivity to PowerBI

3. **Missing ADOMD.NET Assembly**
   - Ensure `lib/Microsoft.AnalysisServices.AdomdClient.dll` exists
   - Install .NET Framework 4.5+ if not present

4. **Memory Errors**
   - The tool automatically implements memory cleanup
   - For large datasets, consider processing fewer months at once

### Debug Mode

Enable detailed logging by modifying `logger/logger.py`:
```python
Logger.setup(name="PowerBI_Export", level=logging.DEBUG)
```
