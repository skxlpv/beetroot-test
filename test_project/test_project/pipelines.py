import os
import pandas as pd
from loguru import logger
from openpyxl import load_workbook

class PandasXlsxPipeline:
    def __init__(self):
        self.items = []
        self.filename = os.path.join(os.getcwd(), "Data Entry - Advanced Content Scraper.xlsx")

    def process_item(self, item, spider):
        self.items.append(item)
        return item

    def close_spider(self, spider):
        if not self.items:
            logger.warning("No items to save.")
            return

        df_new = pd.DataFrame(self.items)

        if not os.path.exists(self.filename):
            logger.error(f"File not found: {self.filename}")
            return

        try:
            book = load_workbook(self.filename)
            sheet_name = book.sheetnames[0]
            ws = book[sheet_name]

            last_data_row = 0
            for row in range(ws.max_row, 0, -1):
                if any(cell.value not in [None, ""] for cell in ws[row]):
                    last_data_row = row
                    break

            header_rows_to_keep = 2
            start_row = max(last_data_row, header_rows_to_keep)

            with pd.ExcelWriter(
                self.filename,
                engine="openpyxl",
                mode="a",
                if_sheet_exists="overlay"
            ) as writer:
                df_new.to_excel(
                    writer,
                    sheet_name=sheet_name,
                    index=False,
                    header=False,
                    startrow=start_row
                )

            logger.info(f"Appended {len(df_new)} rows to {self.filename}")

        except Exception as e:
            logger.error(f"Failed to write data: {e}")


class FilteredAbstractAuthorPipeline:
    def __init__(self):
        self.items = []
        self.filename = os.path.join(os.getcwd(), "Data Entry - Filtered Authors.xlsx")

    def process_item(self, item, spider):
        self.items.append(item)
        return item

    def close_spider(self, spider):
        if not self.items:
            logger.warning("No Abstract authors to save.")
            return

        df = pd.DataFrame(self.items)

        if not os.path.exists(self.filename):
            logger.info(f"Creating new file: {self.filename}")
        else:
            logger.info(f"Overwriting existing file: {self.filename}")

        try:
            df.to_excel(self.filename, index=False)
            logger.info(f"Saved {len(df)} Abstract authors to {self.filename}")
        except Exception as e:
            logger.error(f"Failed to save filtered data: {e}")
