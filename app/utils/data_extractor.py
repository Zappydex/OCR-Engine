##Verified...Now...##

import re
from typing import Dict, List, Tuple, Optional
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
import logging
from google.cloud import vision
from app.models import Invoice, Vendor, Address, InvoiceItem
from app.config import settings
import asyncio
from concurrent.futures import ThreadPoolExecutor
import dateparser
from price_parser import Price
import time
from tenacity import retry, stop_after_attempt, wait_exponential
import aioredis

logger = logging.getLogger(__name__)

class DataExtractor:
    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=settings.MAX_WORKERS)
        self.redis = None

    async def initialize(self):
        self.redis = await aioredis.from_url(settings.REDIS_URL)
    
    async def extract_data(self, ocr_results: List[Dict]) -> List[Invoice]:
        try:
            start_time = time.time()
            results = await asyncio.gather(*[self._extract_single_result(result) for result in ocr_results])
            end_time = time.time()
            logger.info(f"Extracted data for {len(ocr_results)} documents in {end_time - start_time:.2f} seconds")
            return results
        except Exception as e:
            logger.error(f"Error extracting data: {str(e)}")
            return [Invoice(filename=result.get("filename", "")) for result in ocr_results]
    
    async def _extract_date(self, text: str, entities: Optional[List[str]] = None) -> Optional[date]:
        if entities:
            entity_date = await self._extract_date_from_entities(entities)
            if entity_date:
                return entity_date
        
        date_patterns = [
            r'\b(\d{1,2}[/\.-]\d{1,2}[/\.-]\d{2,4})\b',
            r'\b(\d{4}[/\.-]\d{1,2}[/\.-]\d{1,2})\b',
            r'\b(\d{8})\b',
            r'\b(\d{1,2}\s+[A-Za-z]{3,9}\.?\s+\d{2,4})\b',
            r'\b([A-Za-z]{3,9}\.?\s+\d{1,2},?\s+\d{2,4})\b',
            r'\b([A-Za-z]{3}\.?\s+[A-Za-z]{3}\.?\s+\d{2,4})\b',
            r'\b(\d{1,2}\.\d{1,2}\.\d{2,4})\b',
            r'\b(\d{1,2}-\d{1,2}-\d{2,4})\b',
            r'\b(\d{1,2}\s+\d{1,2}\s+\d{2,4})\b',
            r'\b(\d{4}\d{2}\d{2})\b',
            r'\b(\d{2}\d{2}\d{4})\b'
        ]
        
        date_keywords = [
            'date', 'invoice date', 'issue date', 'dated', 'invoice', 
            'issued', 'due date', 'billing date', 'transaction date',
            'document date', 'statement date', 'posting date'
        ]
        
        for keyword in date_keywords:
            keyword_pattern = rf'(?i){re.escape(keyword)}[:\s]*(.{{0,50}})'
            keyword_matches = re.finditer(keyword_pattern, text)
            
            for match in keyword_matches:
                nearby_text = match.group(1)
                
                for pattern in date_patterns:
                    date_matches = re.finditer(pattern, nearby_text)
                    for date_match in date_matches:
                        date_str = date_match.group(0)
                        
                        for date_order in ['DMY', 'MDY', 'YMD']:
                            try:
                                parsed_date = await asyncio.to_thread(
                                    dateparser.parse,
                                    date_str,
                                    settings={
                                        'DATE_ORDER': date_order,
                                        'PREFER_DAY_OF_MONTH': 'first',
                                        'RELATIVE_BASE': datetime.now(),
                                        'PREFER_DATES_FROM': 'past'
                                    }
                                )
                                if parsed_date:
                                    return parsed_date.date()
                            except Exception:
                                pass
        
        for pattern in date_patterns:
            matches = re.finditer(pattern, text)
            for match in matches:
                date_str = match.group(0)
                
                for date_order in ['DMY', 'MDY', 'YMD']:
                    try:
                        parsed_date = await asyncio.to_thread(
                            dateparser.parse,
                            date_str,
                            settings={
                                'DATE_ORDER': date_order,
                                'PREFER_DAY_OF_MONTH': 'first',
                                'RELATIVE_BASE': datetime.now(),
                                'PREFER_DATES_FROM': 'current_period'
                            }
                        )
                        if parsed_date:
                            return parsed_date.date()
                    except Exception:
                        pass
        
        special_date_formats = [
            r'(\d{4})(\d{2})(\d{2})',
            r'(\d{2})(\d{2})(\d{4})'
        ]
        
        for pattern in special_date_formats:
            matches = re.finditer(pattern, text)
            for match in matches:
                if pattern == r'(\d{4})(\d{2})(\d{2})':
                    year, month, day = match.groups()
                    try:
                        return date(int(year), int(month), int(day))
                    except ValueError:
                        pass
                else:
                    first, second, year = match.groups()
                    try:
                        return date(int(year), int(second), int(first))
                    except ValueError:
                        try:
                            return date(int(year), int(first), int(second))
                        except ValueError:
                            pass
        
        month_abbr = {'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6, 
                      'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12}
        
        for month_name, month_num in month_abbr.items():
            pattern = rf'(?i){month_name}\S*\.?\s+(\d{{1,2}})\S*\.?\s+(\d{{4}})'
            matches = re.finditer(pattern, text)
            for match in matches:
                day, year = match.groups()
                try:
                    return date(int(year), month_num, int(day))
                except ValueError:
                    pass
            
            pattern = rf'(?i)(\d{{1,2}})\S*\.?\s+{month_name}\S*\.?\s+(\d{{4}})'
            matches = re.finditer(pattern, text)
            for match in matches:
                day, year = match.groups()
                try:
                    return date(int(year), month_num, int(day))
                except ValueError:
                    pass
        
        dot_date_pattern = r'\b(\d{1,2})\.(\d{1,2})\.(\d{2})\b'
        dot_matches = re.findall(dot_date_pattern, text)
        for match in dot_matches:
            if len(match) == 3:
                day, month, year_short = match
                current_year = datetime.now().year
                century = current_year // 100
                year = int(f"{century}{year_short}")
                if year > current_year + 20:
                    year = int(f"{century-1}{year_short}")
                try:
                    return date(year, int(month), int(day))
                except ValueError:
                    try:
                        return date(year, int(day), int(month))
                    except ValueError:
                        pass
        
        try:
            parsed_date = await asyncio.to_thread(
                dateparser.parse,
                text,
                settings={
                    'RELATIVE_BASE': datetime.now()
                }
            )
            if parsed_date:
                return parsed_date.date()
        except Exception:
            pass
            
        return None
    
    async def _extract_date_from_entities(self, entities: List[str]) -> Optional[date]:
        for entity in entities:
            if entity.startswith('invoice_date:') or entity.startswith('date:'):
                date_str = entity.split(':', 1)[1].strip()
                
                # Direct handling for DD/MM/YYYY format
                if re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', date_str):
                    try:
                        day, month, year = date_str.split('/')
                        return date(int(year), int(month), int(day))
                    except (ValueError, IndexError):
                        pass
                
                # Direct handling for DD-MM-YYYY format
                if re.match(r'^\d{1,2}-\d{1,2}-\d{4}$', date_str):
                    try:
                        day, month, year = date_str.split('-')
                        return date(int(year), int(month), int(day))
                    except (ValueError, IndexError):
                        pass
                
                # Direct handling for YYYY-MM-DD format
                if re.match(r'^\d{4}-\d{1,2}-\d{1,2}$', date_str):
                    try:
                        year, month, day = date_str.split('-')
                        return date(int(year), int(month), int(day))
                    except (ValueError, IndexError):
                        pass
                
                for date_order in ['DMY', 'MDY', 'YMD']:
                    try:
                        parsed_date = await asyncio.to_thread(
                            dateparser.parse,
                            date_str,
                            settings={
                                'DATE_ORDER': date_order,
                                'PREFER_DAY_OF_MONTH': 'first',
                                'RELATIVE_BASE': datetime.now(),
                                'PREFER_DATES_FROM': 'past'
                            }
                        )
                        if parsed_date:
                            return parsed_date.date()
                    except Exception:
                        pass
                
                dot_date_pattern = r'\b(\d{1,2})\.(\d{1,2})\.(\d{2})\b'
                dot_matches = re.findall(dot_date_pattern, date_str)
                for match in dot_matches:
                    if len(match) == 3:
                        day, month, year_short = match
                        current_year = datetime.now().year
                        century = current_year // 100
                        year = int(f"{century}{year_short}")
                        if year > current_year + 20:
                            year = int(f"{century-1}{year_short}")
                        try:
                            return date(year, int(month), int(day))
                        except ValueError:
                            try:
                                return date(year, int(day), int(month))
                            except ValueError:
                                pass
        return None    
    
    async def _extract_single_result(self, ocr_result: Dict) -> Invoice:
        try:
            # Disable cache lookup temporarily to force reprocessing
            # cache_key = f"extracted:{hash(str(ocr_result))}"
            # if self.redis:
            #     cached_result = await self.redis.get(cache_key)
            #     if cached_result:
            #         logger.info(f"Cache hit for {ocr_result.get('filename', '')}")
            #         return Invoice.parse_raw(cached_result)

            # Process the document regardless of cache status
            start_time = time.time()
            invoice = await self.extract_invoice_data(ocr_result)
            end_time = time.time()
            logger.info(f"Extracted data for {ocr_result.get('filename', '')} in {end_time - start_time:.2f} seconds")

            # Disable cache storage temporarily
            # if self.redis:
            #     await self.redis.set(cache_key, invoice.json(), expire=86400)
            
            return invoice
        except Exception as e:
            logger.error(f"Error extracting data for {ocr_result.get('filename', '')}: {str(e)}")
            return Invoice(filename=ocr_result.get("filename", ""))    
    
    async def extract_invoice_data(self, ocr_result: Dict, docai_result: Optional[Dict] = None) -> Invoice:
        filename = ocr_result.get('filename', '')
        
        if docai_result and 'entities' in docai_result:
            logger.info(f"Using DocAI extraction for {filename}")
            invoice = await self._extract_from_docai(docai_result, filename)
            
            if self._is_invoice_valid(invoice):
                logger.info(f"DocAI extraction successful for {filename}, invoice date: {invoice.invoice_date}")
                return invoice
            else:
                logger.warning(f"DocAI extraction produced invalid invoice for {filename}")
        else:
            logger.info(f"No DocAI result available for {filename}, using GCV extraction")
        
        logger.info(f"Falling back to GCV extraction for {filename}")
        return await self._extract_from_gcv(ocr_result, filename)
    
    def _is_invoice_valid(self, invoice: Invoice) -> bool:
        return (invoice.invoice_number or 
                invoice.vendor.name or 
                invoice.invoice_date or 
                invoice.grand_total is not None)
    
    async def _extract_from_docai(self, docai_result: Dict, filename: str) -> Invoice:
        entities = docai_result.get('entities', {})
        
        vendor = Vendor(
            name=entities.get('supplier_name', ''),
            address=Address(
                street=entities.get('supplier_address', ''),
                city=entities.get('supplier_city', ''),
                state=entities.get('supplier_state', ''),
                country=entities.get('supplier_country', ''),
                postal_code=entities.get('supplier_zip', '')
            )
        )

        invoice_date = None
        if 'invoice_date' in entities:
            date_str = entities.get('invoice_date', '')
            logger.info(f"Attempting to parse invoice date: {date_str}")
            try:
                if re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', date_str):
                    try:
                        day, month, year = date_str.split('/')
                        invoice_date = date(int(year), int(month), int(day))
                        logger.info(f"Parsed date directly: {invoice_date}")
                    except (ValueError, IndexError) as e:
                        logger.warning(f"Failed direct parsing: {str(e)}")
                
                elif re.match(r'^\d{1,2}-\d{1,2}-\d{4}$', date_str):
                    try:
                        day, month, year = date_str.split('-')
                        invoice_date = date(int(year), int(month), int(day))
                        logger.info(f"Parsed hyphen date directly: {invoice_date}")
                    except (ValueError, IndexError) as e:
                        logger.warning(f"Failed direct hyphen parsing: {str(e)}")
                
                if not invoice_date:
                    invoice_date_entity = [f"invoice_date:{date_str}"]
                    logger.info(f"Created entity: {invoice_date_entity}")
                    invoice_date = await self._extract_date(date_str, entities=invoice_date_entity)
                    logger.info(f"Result of flexible date extraction: {invoice_date}")
                
                if not invoice_date:
                    logger.warning(f"Could not parse invoice date: {date_str}")
            except Exception as e:
                logger.warning(f"Error parsing invoice date: {date_str}, error: {str(e)}")

        grand_total = None
        if 'net_amount' in entities:
            try:
                grand_total = self._parse_decimal(entities.get('net_amount', ''))
                logger.info(f"Extracted grand_total from net_amount: {grand_total}")
            except Exception as e:
                logger.warning(f"Error parsing net_amount: {str(e)}")
                
        taxes = None
        if 'total_tax_amount' in entities:
            try:
                taxes = self._parse_decimal(entities.get('total_tax_amount', ''))
                logger.info(f"Extracted taxes: {taxes}")
            except Exception as e:
                logger.warning(f"Error parsing total_tax_amount: {str(e)}")
                
        final_total = None
        if 'total_amount' in entities:
            try:
                final_total = self._parse_decimal(entities.get('total_amount', ''))
                logger.info(f"Extracted final_total from total_amount: {final_total}")
            except Exception as e:
                logger.warning(f"Error parsing total_amount: {str(e)}")

        if grand_total is not None and taxes is not None and final_total is not None:
            calculated_total = grand_total + taxes
            if abs(calculated_total - final_total) > Decimal('0.01'):
                logger.warning(f"Total mismatch for {filename}: {grand_total} + {taxes} = {calculated_total}, but final_total is {final_total}")
        
        if grand_total is None and final_total is not None and taxes is not None:
            grand_total = final_total - taxes
            logger.info(f"Calculated grand_total: {grand_total}")
        
        if final_total is None and grand_total is not None and taxes is not None:
            final_total = grand_total + taxes
            logger.info(f"Calculated final_total: {final_total}")
            
        if grand_total is None and 'total_amount' in entities and taxes is not None:
            try:
                total_amount = self._parse_decimal(entities.get('total_amount', ''))
                grand_total = total_amount - taxes
                logger.info(f"Calculated grand_total from total_amount - taxes: {grand_total}")
            except Exception as e:
                logger.warning(f"Error calculating grand_total from total_amount - taxes: {str(e)}")

        items = []
        line_item_entities = []
        document = docai_result.get('document', None)
        
        if document and hasattr(document, 'entities'):
            for entity in document.entities:
                if entity.type_ == 'line_item':
                    line_item_entities.append(entity.mention_text)
        
        for line_item in line_item_entities:
            try:
                item = self._parse_line_item(line_item)
                if item:
                    items.append(item)
            except Exception as e:
                logger.warning(f"Error parsing line item '{line_item}': {str(e)}")
        
        if not items:
            tables = docai_result.get('tables', [])
            for table in tables:
                header_row = None
                if len(table) > 0:
                    header_row = self._identify_header_row(table[0])
                
                for row_idx, row in enumerate(table):
                    if row_idx == 0 and header_row:
                        continue
                    
                    try:
                        item = self._extract_item_from_table_row(row, header_row)
                        if item:
                            items.append(item)
                    except Exception as e:
                        logger.warning(f"Error parsing invoice item from table: {str(e)}")

        return Invoice(
            filename=filename,
            invoice_number=entities.get('invoice_id', ''),
            vendor=vendor,
            invoice_date=invoice_date,
            grand_total=grand_total,
            taxes=taxes,
            final_total=final_total,
            items=items,
            pages=1  
        )
        
    def _parse_line_item(self, line_item: str) -> Optional[InvoiceItem]:
        line = line_item.strip()
        if not line:
            return None
            
        description = None
        quantity = None
        unit_price = None
        total = None
        
        # Try to identify if this is a standard format with quantity at start
        qty_match = re.match(r'^\s*(\d+)\s+(.+)', line)
        if qty_match:
            quantity = int(qty_match.group(1))
            remaining = qty_match.group(2).strip()
            
            # Find all decimal numbers in the string
            amount_matches = list(re.finditer(r'\b(\d+(?:[.,]\d+)?)\b', remaining))
            
            if len(amount_matches) >= 2:
                # Extract the last two numbers as unit price and total
                total_match = amount_matches[-1]
                unit_price_match = amount_matches[-2]
                
                total = self._parse_decimal(total_match.group(1))
                unit_price = self._parse_decimal(unit_price_match.group(1))
                
                # Extract description (everything between quantity and unit price)
                description_end = unit_price_match.start()
                description = remaining[:description_end].strip()
            else:
                # If we can't find unit price and total, just use the description and quantity
                description = remaining
        else:
            # Try to identify if this is a format with description first
            # Find all decimal numbers in the string
            amount_matches = list(re.finditer(r'\b(\d+(?:[.,]\d+)?)\b', line))
            
            if len(amount_matches) >= 3:
                # Might be: DESCRIPTION QTY UNIT_PRICE TOTAL
                qty_match = amount_matches[0]
                unit_price_match = amount_matches[-2]
                total_match = amount_matches[-1]
                
                try:
                    quantity = int(qty_match.group(1))
                    unit_price = self._parse_decimal(unit_price_match.group(1))
                    total = self._parse_decimal(total_match.group(1))
                    
                    # Description is everything before the first number
                    description = line[:qty_match.start()].strip()
                except (ValueError, InvalidOperation):
                    pass
            
            if description is None and len(amount_matches) >= 1:
                # Might be just DESCRIPTION TOTAL
                total_match = amount_matches[-1]
                try:
                    total = self._parse_decimal(total_match.group(1))
                    description = line[:total_match.start()].strip()
                except (ValueError, InvalidOperation):
                    pass
        
        # If we still don't have a description, use the whole line
        if description is None:
            description = line
            
        # Validate the item has at least a description
        if description:
            return InvoiceItem(
                description=description,
                quantity=quantity,
                unit_price=unit_price,
                total=total
            )
        
        return None
        
    def _identify_header_row(self, row: List[str]) -> Dict[str, int]:
        header_map = {}
        for idx, cell in enumerate(row):
            cell_lower = cell.lower().strip()
            
            if any(kw in cell_lower for kw in ['desc', 'item', 'service', 'product']):
                header_map['description'] = idx
            elif any(kw in cell_lower for kw in ['qty', 'quantity', 'count', 'units']):
                header_map['quantity'] = idx
            elif any(kw in cell_lower for kw in ['price', 'rate', 'unit', 'cost']):
                header_map['unit_price'] = idx
            elif any(kw in cell_lower for kw in ['amount', 'total', 'sum']):
                header_map['total'] = idx
                
        return header_map
        
    def _extract_item_from_table_row(self, row: List[str], header_map: Optional[Dict[str, int]] = None) -> Optional[InvoiceItem]:
        if not row:
            return None
            
        description = None
        quantity = None
        unit_price = None
        total = None
        
        if header_map:
            # Extract values based on identified headers
            if 'description' in header_map and header_map['description'] < len(row):
                description = row[header_map['description']].strip()
                
            if 'quantity' in header_map and header_map['quantity'] < len(row):
                qty_str = row[header_map['quantity']].strip()
                try:
                    if qty_str and qty_str.replace('.', '', 1).isdigit():
                        quantity = int(float(qty_str))
                except (ValueError, TypeError):
                    pass
                    
            if 'unit_price' in header_map and header_map['unit_price'] < len(row):
                price_str = row[header_map['unit_price']].strip()
                try:
                    if price_str:
                        unit_price = self._parse_decimal(price_str)
                except (ValueError, InvalidOperation):
                    pass
                    
            if 'total' in header_map and header_map['total'] < len(row):
                total_str = row[header_map['total']].strip()
                try:
                    if total_str:
                        total = self._parse_decimal(total_str)
                except (ValueError, InvalidOperation):
                    pass
        else:
            # No header map, try to infer based on position and content
            if len(row) >= 4:
                description = row[0].strip()
                
                try:
                    qty_str = row[1].strip()
                    if qty_str and qty_str.replace('.', '', 1).isdigit():
                        quantity = int(float(qty_str))
                except (ValueError, TypeError):
                    pass
                    
                try:
                    if row[2].strip():
                        unit_price = self._parse_decimal(row[2])
                except (ValueError, InvalidOperation):
                    pass
                    
                try:
                    if row[3].strip():
                        total = self._parse_decimal(row[3])
                except (ValueError, InvalidOperation):
                    pass
            elif len(row) == 3:
                # Might be DESCRIPTION QTY TOTAL
                description = row[0].strip()
                
                try:
                    qty_str = row[1].strip()
                    if qty_str and qty_str.replace('.', '', 1).isdigit():
                        quantity = int(float(qty_str))
                except (ValueError, TypeError):
                    pass
                    
                try:
                    if row[2].strip():
                        total = self._parse_decimal(row[2])
                except (ValueError, InvalidOperation):
                    pass
            elif len(row) == 2:
                # Might be DESCRIPTION TOTAL
                description = row[0].strip()
                
                try:
                    if row[1].strip():
                        total = self._parse_decimal(row[1])
                except (ValueError, InvalidOperation):
                    pass
        
        # Validate the item has at least a description
        if description:
            return InvoiceItem(
                description=description,
                quantity=quantity,
                unit_price=unit_price,
                total=total
            )
            
        return None 
    
    async def _extract_from_gcv(self, ocr_result: Dict, filename: str) -> Invoice:
        text = ocr_result.get('text', '')
        if not text and 'words' in ocr_result:
            text = ' '.join(ocr_result.get('words', []))
        
        invoice_number = self._extract_invoice_number(text)
        
        vendor = self._extract_vendor(text)
        
        invoice_date = await self._extract_date(text) 
        
        grand_total, taxes, final_total = self._extract_totals(text)
        
        items = self._extract_items(ocr_result)
        
        return Invoice(
            filename=filename,
            invoice_number=invoice_number,
            vendor=vendor,
            invoice_date=invoice_date,
            grand_total=grand_total,
            taxes=taxes,
            final_total=final_total,
            items=items,
            pages=ocr_result.get('num_pages', 1)
        )

    def _extract_invoice_number(self, text: str) -> Optional[str]:
        patterns = [
            r'(?i)invoice\s*number?[:\s]*([A-Za-z0-9-]{5,})',
            r'(?i)invoice\s*#[:\s]*([A-Za-z0-9-]{5,})',
            r'(?i)inv[:\s]*([A-Za-z0-9-]{5,})'
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1)
        return None

    def _extract_vendor(self, text: str) -> Vendor:
        lines = text.split('\n')
        if not lines:
            return Vendor(name="", address=Address())
            
        name = lines[0] if lines else ""
        address_text = '\n'.join(lines[1:4]) if len(lines) > 1 else ""
        
        return Vendor(
            name=name,
            address=self._extract_address(address_text)
        )

    def _extract_address(self, text: str) -> Address:
        lines = text.split('\n')
        
        street = lines[0] if lines else ""
        city = ""
        state = ""
        postal_code = ""
        country = ""
        
        if len(lines) > 1:
            address_line = lines[1]
            postal_match = re.search(r'\b\d{5}(?:-\d{4})?\b', address_line)
            if postal_match:
                postal_code = postal_match.group(0)
            
            city_state_match = re.search(r'([A-Za-z\s]+),\s*([A-Z]{2})', address_line)
            if city_state_match:
                city = city_state_match.group(1).strip()
                state = city_state_match.group(2)
        
        return Address(
            street=street,
            city=city,
            state=state,
            country=country,
            postal_code=postal_code
        )  

    def _extract_totals(self, text: str) -> Tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal]]:
        grand_total = None
        taxes = None
        final_total = None
        
        subtotal_match = re.search(r'(?i)subtotal[:\s]*\$?([\d,]+\.\d{2})', text)
        if subtotal_match:
            grand_total = self._parse_decimal(subtotal_match.group(1))
        
        tax_match = re.search(r'(?i)tax[:\s]*\$?([\d,]+\.\d{2})', text)
        if tax_match:
            taxes = self._parse_decimal(tax_match.group(1))
        
        total_match = re.search(r'(?i)total[:\s]*\$?([\d,]+\.\d{2})', text)
        if total_match:
            final_total = self._parse_decimal(total_match.group(1))
        
        return grand_total, taxes, final_total

    def _extract_items(self, ocr_result: Dict) -> List[InvoiceItem]:
        items = []
        
        tables = ocr_result.get('tables', [])
        for table in tables:
            for row in table[1:] if len(table) > 1 else []:
                try:
                    if len(row) >= 4:
                        description = row[0]
                        quantity = int(row[1]) if row[1].strip() else None
                        unit_price = self._parse_decimal(row[2]) if row[2].strip() else None
                        total = self._parse_decimal(row[3]) if row[3].strip() else None
                        
                        items.append(InvoiceItem(
                            description=description,
                            quantity=quantity,
                            unit_price=unit_price,
                            total=total
                        ))
                except (ValueError, IndexError, InvalidOperation) as e:
                    logger.warning(f"Error parsing item: {str(e)}")
        
        return items

    def _parse_decimal(self, amount_string: str) -> Optional[Decimal]:
        if not amount_string or not amount_string.strip():
            return None
            
        try:
            cleaned = re.sub(r'[^\d.-]', '', amount_string)
            return Decimal(cleaned)
        except (InvalidOperation, TypeError):
            try:
                price = Price.fromstring(amount_string)
                return Decimal(str(price.amount)) if price.amount else None
            except:
                logger.warning(f"Could not parse decimal: {amount_string}")
                return None
            
    async def cleanup(self):
        self.executor.shutdown(wait=True)
        if self.redis:
            await self.redis.close()

data_extractor = DataExtractor()

async def initialize_data_extractor():
    await data_extractor.initialize()

async def cleanup_data_extractor():
    await data_extractor.cleanup()

async def extract_invoice_data(ocr_result: Dict, docai_result: Optional[Dict] = None) -> Invoice:
    return await data_extractor.extract_invoice_data(ocr_result, docai_result)
