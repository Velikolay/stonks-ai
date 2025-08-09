from dotenv import load_dotenv
from edgar import Company, download_filings

# from edgar.xbrl import XBRLS

load_dotenv()

company = Company("AAPL")

filings = company.get_filings(form="10-Q")

download_filings(filings=filings)

# Parse XBRL data
# xbrls = XBRLS.from_filings(filings)
xbrl = filings.latest().xbrl()

statements = xbrl.facts.get_facts_with_dimensions

# Query by concept
# revenue_query = facts.get_facts_by_concept("Revenue")
# print(revenue_query)

# Display financial statements
# balance_sheet = statements.balance_sheet()
income_statement = statements.income_statement()
# cash_flow = statements.cashflow_statement()
print(statements)

# revenue_trend = income_statement.to_dataframe()

# Print the full DataFrame to see all data
# print("Full revenue_trend DataFrame:")
# print("=" * 50)
# print(revenue_trend)
# print("\n" + "=" * 50)

# # Print DataFrame info (columns, data types, etc.)
# print("\nDataFrame Info:")
# print("=" * 30)
# print(revenue_trend.info())

# revenue_row = revenue_trend.loc[revenue_trend['label'] == 'Contract Revenue']

# # Print revenue_row with all columns
# print("\nRevenue Row DataFrame:")
# print("=" * 40)
# print("DataFrame Shape:", revenue_row.shape)
# print("Columns:", list(revenue_row.columns))
# print("\nFull revenue_row DataFrame:")
# print("=" * 50)
# print(revenue_row.to_string(index=False, max_colwidth=100))
