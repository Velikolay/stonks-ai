"""Add concept normalization overrides table

Revision ID: 0003
Revises: 0002
Create Date: 2025-01-27 10:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create concept normalization mapping table
    op.create_table(
        "concept_normalization_overrides",
        sa.Column("concept", sa.String(), nullable=False),
        sa.Column("statement", sa.String(), nullable=False),
        sa.Column("normalized_label", sa.String(), nullable=False),
        sa.Column("is_abstract", sa.Boolean(), nullable=False),
        sa.Column("parent_concept", sa.String(), nullable=True),
        sa.Column("description", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("concept", "statement"),
        sa.ForeignKeyConstraint(
            ["parent_concept", "statement"],
            [
                "concept_normalization_overrides.concept",
                "concept_normalization_overrides.statement",
            ],
            name="fk_concept_normalization_overrides_parent_concept_statement",
        ),
    )

    # Insert initial concept mappings for common financial metrics
    op.execute(
        """
        INSERT INTO concept_normalization_overrides (concept, normalized_label, statement, is_abstract, parent_concept) VALUES

        -- Revenue
        ('us-gaap:Revenues', 'Revenue', 'Income Statement', False, NULL),
        ('us-gaap:SalesRevenueNet', 'Revenue', 'Income Statement', False, NULL),
        ('us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax', 'Revenue', 'Income Statement', False, NULL),
        ('us-gaap:RevenueFromContractWithCustomerIncludingAssessedTax', 'Revenue', 'Income Statement', False, NULL),
        ('us-gaap:OperatingRevenues', 'Revenue', 'Income Statement', False, NULL),

        -- Cost of Revenue
        ('us-gaap:CostOfGoodsAndServicesSold', 'Cost of Revenue', 'Income Statement', False, NULL),
        ('us-gaap:CostOfRevenue', 'Cost of Revenue', 'Income Statement', False, NULL),
        ('us-gaap:CostOfGoodsSold', 'Cost of Revenue', 'Income Statement', False, NULL),

        -- Gross Profit
        ('us-gaap:GrossProfit', 'Gross Profit', 'Income Statement', False, NULL),
        ('us-gaap:GrossProfitLoss', 'Gross Profit', 'Income Statement', False, NULL),

        -- Operating Income
        ('us-gaap:OperatingIncomeLoss', 'Operating Income', 'Income Statement', False, NULL),
        ('us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxes', 'Operating Income', 'Income Statement', False, NULL),

        -- Net Income
        ('us-gaap:NetIncomeLoss', 'Net Income', 'Income Statement', False, NULL),
        ('us-gaap:ProfitLoss', 'Net Income', 'Income Statement', False, NULL),
        ('us-gaap:IncomeLossFromContinuingOperationsAfterIncomeTaxes', 'Net Income', 'Income Statement', False, NULL),

        -- Income Before Tax from Continuing Operations
        ('us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest', 'Income Before Tax from Continuing Operations', 'Income Statement', False, NULL),
        ('us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments', 'Income Before Tax from Continuing Operations', 'Income Statement', False, NULL),

        -- Cash and Cash Equivalents
        ('us-gaap:CashAndCashEquivalentsAtCarryingValue', 'Cash and Cash Equivalents', 'Balance Sheet', False, NULL),
        ('us-gaap:Cash', 'Cash and Cash Equivalents', 'Balance Sheet', False, NULL),
        ('us-gaap:CashEquivalentsAtCarryingValue', 'Cash and Cash Equivalents', 'Balance Sheet', False, NULL),

        -- Total Assets
        ('us-gaap:Assets', 'Total Assets', 'Balance Sheet', False, NULL),
        ('us-gaap:AssetsTotal', 'Total Assets', 'Balance Sheet', False, NULL),

        -- Total Liabilities
        ('us-gaap:Liabilities', 'Total Liabilities', 'Balance Sheet', False, NULL),
        ('us-gaap:LiabilitiesTotal', 'Total Liabilities', 'Balance Sheet', False, NULL),

        -- Total Equity
        ('us-gaap:StockholdersEquity', 'Total Equity', 'Balance Sheet', False, NULL),
        ('us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest', 'Total Equity', 'Balance Sheet', False, NULL),
        ('us-gaap:Equity', 'Total Equity', 'Balance Sheet', False, NULL),

        -- Operating Cash Flow
        ('us-gaap:NetCashProvidedByUsedInOperatingActivities', 'Operating Cash Flow', 'Cash Flow Statement', False, NULL),
        ('us-gaap:NetCashProvidedByUsedInOperatingActivitiesContinuingOperations', 'Operating Cash Flow', 'Cash Flow Statement', False, NULL),

        -- Investing Cash Flow
        ('us-gaap:NetCashProvidedByUsedInInvestingActivities', 'Investing Cash Flow', 'Cash Flow Statement', False, NULL),
        ('us-gaap:NetCashProvidedByUsedInInvestingActivitiesContinuingOperations', 'Investing Cash Flow', 'Cash Flow Statement', False, NULL),

        -- Financing Cash Flow
        ('us-gaap:NetCashProvidedByUsedInFinancingActivities', 'Financing Cash Flow', 'Cash Flow Statement', False, NULL),
        ('us-gaap:NetCashProvidedByUsedInFinancingActivitiesContinuingOperations', 'Financing Cash Flow', 'Cash Flow Statement', False, NULL),

        -- Free Cash Flow
        ('us-gaap:FreeCashFlow', 'Free Cash Flow', 'Cash Flow Statement', False, NULL),

        -- EPS
        ('us-gaap:EarningsPerShareBasic', 'Basic EPS', 'Income Statement', False, NULL),
        ('us-gaap:EarningsPerShareDiluted', 'Diluted EPS', 'Income Statement', False, NULL),

        -- Cash Dividents Per Share
        ('us-gaap:CommonStockDividendsPerShareDeclared', 'Cash Dividends Per Share', 'Income Statement', False, NULL),

        -- Debt concepts
        ('us-gaap:LongTermDebt', 'Long-term Debt', 'Balance Sheet', False, NULL),
        ('us-gaap:LongTermDebtNoncurrent', 'Long-term Debt', 'Balance Sheet', False, NULL),
        ('us-gaap:ShortTermBorrowings', 'Short-term Debt', 'Balance Sheet', False, NULL),
        ('us-gaap:ShortTermDebt', 'Short-term Debt', 'Balance Sheet', False, NULL),

        -- Current Marketable Securities
        ('us-gaap:MarketableSecuritiesCurrent', 'Current Marketable Securities', 'Balance Sheet', False, NULL),
        ('us-gaap:AvailableForSaleSecuritiesCurrent', 'Current Marketable Securities', 'Balance Sheet', False, NULL),

        -- Non-current Marketable Securities
        ('us-gaap:MarketableSecuritiesNoncurrent', 'Non-current Marketable Securities', 'Balance Sheet', False, NULL),
        ('us-gaap:AvailableForSaleSecuritiesNoncurrent', 'Non-current Marketable Securities', 'Balance Sheet', False, NULL),
        ('us-gaap:AvailableForSaleSecuritiesDebtSecuritiesNoncurrent', 'Non-current Marketable Securities', 'Balance Sheet', False, NULL),

        -- Deferred Revenue
        ('us-gaap:DeferredRevenueCurrent', 'Deferred Revenue', 'Balance Sheet', False, NULL),
        ('us-gaap:ContractWithCustomerLiabilityCurrent', 'Deferred Revenue', 'Balance Sheet', False, NULL),
        ('us-gaap:DeferredRevenueNoncurrent', 'Deferred Revenue Non-current', 'Balance Sheet', False, NULL),

        -- Other
        ('us-gaap:ProceedsFromPaymentsForOtherFinancingActivities', 'Other Financing Activities', 'Cash Flow Statement', False, NULL),
        ('us-gaap:PaymentsForProceedsFromOtherInvestingActivities', 'Other Investing Activities', 'Cash Flow Statement', False, NULL),
        ('us-gaap:OtherNoncashIncomeExpense', 'Other Income Expenses', 'Cash Flow Statement', False, NULL),

        -- Goodwill
        ('us-gaap:Goodwill', 'Goodwill', 'Balance Sheet', False, 'us-gaap:AssetsNoncurrentAbstract'),
        ('us-gaap:IntangibleAssetsNetExcludingGoodwill', 'Intangible Assets Excluding Goodwill', 'Balance Sheet', False, 'us-gaap:AssetsNoncurrentAbstract'),
        ('us-gaap:AssetsNoncurrentAbstract', 'Non-current Assets', 'Balance Sheet', True, 'us-gaap:AssetsAbstract'),
        ('us-gaap:AssetsAbstract', 'Assets', 'Balance Sheet', True, NULL)
    """
    )


def downgrade() -> None:
    # Drop concept_normalization_overrides table
    op.drop_table("concept_normalization_overrides")
