# __init__.py
"""PowerBI Export Pipeline - Modular data extraction system."""

__version__ = "1.0.0"
__author__ = "Hamza CHAQCHAQ"
__description__ = "Automated PowerBI data extraction with 12-month rolling window"

from .main import PowerBIExportPipeline

__all__ = ['PowerBIExportPipeline']