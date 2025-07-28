# utils/validation_manager.py
"""Validation manager for comparing DAX query results with DataFrame calculations."""

import os
from datetime import datetime
from typing import Dict, List

import pandas as pd


class ValidationManager:
    """Manages validation of extracted data against DAX query results."""

    def __init__(self, output_dir: str):
        """
        Initialize validation manager.

        Args:
            output_dir: Directory for validation output files
        """
        self.output_dir = output_dir
        self.validation_results: List[Dict] = []
        self._ensure_directory()

    def _ensure_directory(self):
        """Create output directory if it doesn't exist."""
        validation_dir = os.path.join(self.output_dir, "validation")
        os.makedirs(validation_dir, exist_ok=True)
        self.validation_dir = validation_dir

    def validate_month(
            self,
            year: int,
            month: int,
            dax_sum: float,
            dataframe_sum: float,
            row_count: int,
            month_name: str
    ) -> Dict:
        """
        Validate DAX query result against DataFrame sum.

        Args:
            year: Year being validated
            month: Month being validated
            dax_sum: Sum from DAX query
            dataframe_sum: Sum from DataFrame
            row_count: Number of rows in DataFrame
            month_name: Formatted month name

        Returns:
            Validation result dictionary
        """
        # Calculate difference
        difference = abs(dax_sum - dataframe_sum)
        percentage_diff = (difference / dax_sum * 100) if dax_sum != 0 else 0

        # Determine if validation passed (difference less than 0.01%)
        validation_passed = percentage_diff < 0.01

        validation_result = {
            'year': year,
            'month': month,
            'month_name': month_name,
            'dax_sum': dax_sum,
            'dataframe_sum': dataframe_sum,
            'difference': difference,
            'percentage_difference': percentage_diff,
            'row_count': row_count,
            'validation_passed': validation_passed,
            'validation_time': datetime.now().isoformat()
        }

        # Add to results
        self.validation_results.append(validation_result)

        # Print validation status
        status_emoji = "✅" if validation_passed else "❌"
        print(f"\n{status_emoji} Validation for {month_name}:")
        print(f"   • DAX Query Sum: €{dax_sum:,.2f}")
        print(f"   • DataFrame Sum: €{dataframe_sum:,.2f}")
        print(f"   • Difference: €{difference:,.2f} ({percentage_diff:.4f}%)")
        print(f"   • Status: {'PASSED' if validation_passed else 'FAILED'}")

        return validation_result

    def save_validation_report(self):
        """Save validation results to multiple formats."""
        if not self.validation_results:
            print("No validation results to save.")
            return

        # Create DataFrame from results
        df = pd.DataFrame(self.validation_results)

        # Save as Excel with formatting
        excel_path = os.path.join(self.validation_dir,
                                  f"validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Validation Results', index=False)

            # Add summary sheet
            summary_data = {
                'Total Months Validated': len(df),
                'Validations Passed': len(df[df['validation_passed']]),
                'Validations Failed': len(df[~df['validation_passed']]),
                'Total DAX Sum': df['dax_sum'].sum(),
                'Total DataFrame Sum': df['dataframe_sum'].sum(),
                'Total Difference': df['difference'].sum(),
                'Average Percentage Difference': df['percentage_difference'].mean()
            }

            summary_df = pd.DataFrame(list(summary_data.items()), columns=['Metric', 'Value'])
            summary_df.to_excel(writer, sheet_name='Summary', index=False)

        print(f"📊 Validation Excel saved: {excel_path}")
        # Print summary
        self._print_summary()

    def _print_summary(self):
        """Print validation summary."""
        df = pd.DataFrame(self.validation_results)

        print("\n" + "=" * 60)
        print("📊 VALIDATION SUMMARY")
        print("=" * 60)
        print(f"Total Months Validated: {len(df)}")
        print(f"✅ Passed: {len(df[df['validation_passed']])}")
        print(f"❌ Failed: {len(df[~df['validation_passed']])}")
        print(f"\nTotal DAX Sum: €{df['dax_sum'].sum():,.2f}")
        print(f"Total DataFrame Sum: €{df['dataframe_sum'].sum():,.2f}")
        print(f"Total Difference: €{df['difference'].sum():,.2f}")
        print(f"Average % Difference: {df['percentage_difference'].mean():.4f}%")
        print("=" * 60)

        # Show failed validations if any
        failed = df[~df['validation_passed']]
        if len(failed) > 0:
            print("\n⚠️  Failed Validations:")
            for _, row in failed.iterrows():
                print(f"   • {row['month_name']}: {row['percentage_difference']:.4f}% difference")

    def clear(self):
        """Clear validation results."""
        self.validation_results.clear()
