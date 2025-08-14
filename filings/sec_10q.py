from dotenv import load_dotenv
from edgar import Company

# from edgar.xbrl import XBRLS

load_dotenv()

company = Company("AAPL")

filings = company.get_filings(form="10-Q")

# Parse XBRL data
# xbrls = XBRLS.from_filings(filings)
xbrl = filings.latest().xbrl()

df = (
    xbrl.query()
    .by_concept("Revenue")
    .by_dimension("ProductOrServiceAxis")
    .to_dataframe()
)

disaggregated_revenue = df.loc[
    df.groupby("dim_srt_ProductOrServiceAxis")["period_start"].idxmax()
]

print(disaggregated_revenue)
