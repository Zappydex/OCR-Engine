import pandas as pd
import io
import logging
from typing import List
from app.models import Invoice
from app.config import settings
import asyncio
from concurrent.futures import ThreadPoolExecutor
from openpyxl.styles import Border, Side, Alignment, Font

logger = logging.getLogger(__name__)

class InvoiceExporter:
    def __init__(self):
        self.columns = [
            "Filename", "Invoice Number", "Vendor Name", "Address", 
            "Invoice Date", "Grand Total", "Taxes", "Final Total", 
            "Description", "Quantity", "Unit Price", "Total", "Pages"
        ]
        self.executor = ThreadPoolExecutor(max_workers=settings.MAX_WORKERS)
        self.default_currency = '$'  # Default currency symbol

    async def export_invoices(self, invoices: List[Invoice], format: str) -> io.BytesIO:
        try:
            df = await self._create_dataframe(invoices)
            if format.lower() == 'csv':
                return await self._export_to_csv(df)
            elif format.lower() == 'excel':
                return await self._export_to_excel(df)
            else:
                raise ValueError(f"Unsupported export format: {format}")
        except Exception as e:
            logger.error(f"Error during invoice export: {str(e)}")
            raise

    async def _create_dataframe(self, invoices: List[Invoice]) -> pd.DataFrame:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self._create_dataframe_sync, invoices)

    def _create_dataframe_sync(self, invoices: List[Invoice]) -> pd.DataFrame:
        data = []
        for index, invoice in enumerate(invoices, 1):
            # Combine address fields into a single address
            address_parts = [
                invoice.vendor.address.street,
                invoice.vendor.address.city,
                invoice.vendor.address.state,
                invoice.vendor.address.postal_code,
                invoice.vendor.address.country
            ]
            address = ", ".join([part for part in address_parts if part])
            
            # Calculate aggregated values for line items
            total_quantity = 0
            total_amount = 0
            avg_unit_price = 0
            
            if invoice.items:
                for item in invoice.items:
                    if item.quantity is not None:
                        total_quantity += item.quantity
                    if item.total is not None:
                        total_amount += item.total
                
                # Calculate average unit price if we have quantities
                if total_quantity > 0:
                    avg_unit_price = total_amount / total_quantity
            
            # Use "Purchase X" as the description
            description = f"Purchase {index}"
            
            # Format monetary values with currency symbol and 4 decimal places
            grand_total = invoice.grand_total
            if grand_total is not None:
                grand_total = f"{self.default_currency}{grand_total:.4f}"
                
            taxes = invoice.taxes
            if taxes is not None:
                taxes = f"{self.default_currency}{taxes:.4f}"
                
            final_total = invoice.final_total
            if final_total is not None:
                final_total = f"{self.default_currency}{final_total:.4f}"
                
            # Format unit price with 4 decimal places (no currency symbol)
            if avg_unit_price != 0:
                avg_unit_price = f"{avg_unit_price:.4f}"
            
            # Format total with currency symbol and 4 decimal places
            total_formatted = total_amount
            if total_amount != 0:
                total_formatted = f"{self.default_currency}{total_amount:.4f}"
            
            row = {
                "Filename": invoice.filename,
                "Invoice Number": invoice.invoice_number,
                "Vendor Name": invoice.vendor.name,
                "Address": address,
                "Invoice Date": invoice.invoice_date,
                "Grand Total": grand_total,
                "Taxes": taxes,
                "Final Total": final_total,
                "Description": description,
                "Quantity": total_quantity,
                "Unit Price": avg_unit_price,
                "Total": total_formatted,
                "Pages": index  # Use the index as the page number
            }
            data.append(row)

        df = pd.DataFrame(data, columns=self.columns)
        return df

    async def _export_to_csv(self, df: pd.DataFrame) -> io.BytesIO:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self._export_to_csv_sync, df)

    def _export_to_csv_sync(self, df: pd.DataFrame) -> io.BytesIO:
        output = io.BytesIO()
        # Use quoting=csv.QUOTE_ALL to ensure all fields are quoted and displayed properly
        import csv
        df.to_csv(output, index=False, quoting=csv.QUOTE_ALL)
        output.seek(0)
        return output

    async def _export_to_excel(self, df: pd.DataFrame) -> io.BytesIO:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self._export_to_excel_sync, df)

    def _export_to_excel_sync(self, df: pd.DataFrame) -> io.BytesIO:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Invoices', index=False)
            
            workbook = writer.book
            sheet = workbook['Invoices']
            
            # Define thick border style
            thick_border = Border(
                left=Side(style='thick'),
                right=Side(style='thick'),
                top=Side(style='thick'),
                bottom=Side(style='thick')
            )
            
            # Apply formatting to all cells
            for row in sheet.iter_rows():
                for cell in row:
                    cell.border = thick_border
                    cell.alignment = Alignment(wrap_text=True, vertical='center')
                    
            # Make headers bold
            for cell in sheet[1]:
                cell.font = Font(bold=True)
            
            # Auto-adjust column widths
            for column in sheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = (max_length + 2)
                sheet.column_dimensions[column_letter].width = adjusted_width

        output.seek(0)
        return output

async def export_invoices(invoices: List[Invoice], format: str) -> io.BytesIO:
    exporter = InvoiceExporter()
    return await exporter.export_invoices(invoices, format)
