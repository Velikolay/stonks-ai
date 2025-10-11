"""Add automatic concept normalization view

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
        "concept_normalizations",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("concept", sa.String(), nullable=False),
        sa.Column("normalized_label", sa.String(), nullable=False),
        sa.Column(
            "statement", sa.String(), nullable=True
        ),  # Optional: specific to statement type
        sa.Column("description", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "concept", "statement", name="uq_concept_normalizations_concept_statement"
        ),
    )

    # Insert initial concept mappings for common financial metrics
    op.execute(
        """
        INSERT INTO concept_normalizations (concept, normalized_label, statement, description) VALUES
        -- Revenue concepts
        ('us-gaap:Revenues', 'Revenue', 'Income Statement', 'Total revenue'),
        ('us-gaap:SalesRevenueNet', 'Revenue', 'Income Statement', 'Net sales revenue'),
        ('us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax', 'Revenue', 'Income Statement', 'Revenue from contracts with customers'),
        ('us-gaap:RevenueFromContractWithCustomerIncludingAssessedTax', 'Revenue', 'Income Statement', 'Revenue from contracts including tax'),
        ('us-gaap:OperatingRevenues', 'Revenue', 'Income Statement', 'Operating revenues'),

        -- Cost of Revenue concepts
        ('us-gaap:CostOfGoodsAndServicesSold', 'Cost of Revenue', 'Income Statement', 'Cost of goods and services sold'),
        ('us-gaap:CostOfRevenue', 'Cost of Revenue', 'Income Statement', 'Cost of revenue'),
        ('us-gaap:CostOfGoodsSold', 'Cost of Revenue', 'Income Statement', 'Cost of goods sold'),

        -- Gross Profit concepts
        ('us-gaap:GrossProfit', 'Gross Profit', 'Income Statement', 'Gross profit'),
        ('us-gaap:GrossProfitLoss', 'Gross Profit', 'Income Statement', 'Gross profit or loss'),

        -- Operating Income concepts
        ('us-gaap:OperatingIncomeLoss', 'Operating Income', 'Income Statement', 'Operating income or loss'),
        ('us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxes', 'Operating Income', 'Income Statement', 'Income from continuing operations before taxes'),

        -- Net Income concepts
        ('us-gaap:NetIncomeLoss', 'Net Income', 'Income Statement', 'Net income or loss'),
        ('us-gaap:ProfitLoss', 'Net Income', 'Income Statement', 'Profit or loss'),
        ('us-gaap:IncomeLossFromContinuingOperationsAfterIncomeTaxes', 'Net Income', 'Income Statement', 'Income from continuing operations after taxes'),

        -- Income Before Tax from Continuing Operations
        ('us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest', 'Income Before Tax from Continuing Operations', 'Income Statement', 'Income from continuing operations before taxes'),
        ('us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments', 'Income Before Tax from Continuing Operations', 'Income Statement', 'Income from continuing operations before taxes'),

        -- Cash and Cash Equivalents concepts
        ('us-gaap:CashAndCashEquivalentsAtCarryingValue', 'Cash and Cash Equivalents', 'Balance Sheet', 'Cash and cash equivalents'),
        ('us-gaap:Cash', 'Cash and Cash Equivalents', 'Balance Sheet', 'Cash'),
        ('us-gaap:CashEquivalentsAtCarryingValue', 'Cash and Cash Equivalents', 'Balance Sheet', 'Cash equivalents'),

        -- Total Assets concepts
        ('us-gaap:Assets', 'Total Assets', 'Balance Sheet', 'Total assets'),
        ('us-gaap:AssetsTotal', 'Total Assets', 'Balance Sheet', 'Total assets'),

        -- Total Liabilities concepts
        ('us-gaap:Liabilities', 'Total Liabilities', 'Balance Sheet', 'Total liabilities'),
        ('us-gaap:LiabilitiesTotal', 'Total Liabilities', 'Balance Sheet', 'Total liabilities'),

        -- Total Equity concepts
        ('us-gaap:StockholdersEquity', 'Total Equity', 'Balance Sheet', 'Stockholders equity'),
        ('us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest', 'Total Equity', 'Balance Sheet', 'Total stockholders equity'),
        ('us-gaap:Equity', 'Total Equity', 'Balance Sheet', 'Total equity'),

        -- Operating Cash Flow concepts
        ('us-gaap:NetCashProvidedByUsedInOperatingActivities', 'Operating Cash Flow', 'Cash Flow Statement', 'Net cash from operating activities'),
        ('us-gaap:NetCashProvidedByUsedInOperatingActivitiesContinuingOperations', 'Operating Cash Flow', 'Cash Flow Statement', 'Net cash from operating activities - continuing'),

        -- Investing Cash Flow concepts
        ('us-gaap:NetCashProvidedByUsedInInvestingActivities', 'Investing Cash Flow', 'Cash Flow Statement', 'Net cash from investing activities'),
        ('us-gaap:NetCashProvidedByUsedInInvestingActivitiesContinuingOperations', 'Investing Cash Flow', 'Cash Flow Statement', 'Net cash from investing activities - continuing'),

        -- Financing Cash Flow concepts
        ('us-gaap:NetCashProvidedByUsedInFinancingActivities', 'Financing Cash Flow', 'Cash Flow Statement', 'Net cash from financing activities'),
        ('us-gaap:NetCashProvidedByUsedInFinancingActivitiesContinuingOperations', 'Financing Cash Flow', 'Cash Flow Statement', 'Net cash from financing activities - continuing'),

        -- Free Cash Flow concepts
        ('us-gaap:FreeCashFlow', 'Free Cash Flow', 'Cash Flow Statement', 'Free cash flow'),

        -- EPS concepts
        ('us-gaap:EarningsPerShareBasic', 'Basic EPS', 'Income Statement', 'Basic earnings per share'),
        ('us-gaap:EarningsPerShareDiluted', 'Diluted EPS', 'Income Statement', 'Diluted earnings per share'),

        -- Cash Dividents Per Share concepts
        ('us-gaap:CommonStockDividendsPerShareDeclared', 'Cash Dividends Per Share', 'Income Statement', 'Cash dividends declared per share'),

        -- Debt concepts
        ('us-gaap:LongTermDebt', 'Long-term Debt', 'Balance Sheet', 'Long-term debt'),
        ('us-gaap:LongTermDebtNoncurrent', 'Long-term Debt', 'Balance Sheet', 'Long-term debt noncurrent'),
        ('us-gaap:ShortTermBorrowings', 'Short-term Debt', 'Balance Sheet', 'Short-term borrowings'),
        ('us-gaap:ShortTermDebt', 'Short-term Debt', 'Balance Sheet', 'Short-term debt'),

        -- Current Marketable Securities concepts
        ('us-gaap:MarketableSecuritiesCurrent', 'Current Marketable Securities', 'Balance Sheet', 'Marketable securities current'),
        ('us-gaap:AvailableForSaleSecuritiesCurrent', 'Current Marketable Securities', 'Balance Sheet', 'Marketable securities current'),

        -- Non-current Marketable Securities concepts
        ('us-gaap:MarketableSecuritiesNoncurrent', 'Non-current Marketable Securities', 'Balance Sheet', 'Marketable securities non-current'),
        ('us-gaap:AvailableForSaleSecuritiesNoncurrent', 'Non-current Marketable Securities', 'Balance Sheet', 'Marketable securities non-current'),
        ('us-gaap:AvailableForSaleSecuritiesDebtSecuritiesNoncurrent', 'Non-current Marketable Securities', 'Balance Sheet', 'Marketable securities non-current'),

        -- Deferred Revenue concepts
        ('us-gaap:DeferredRevenueCurrent', 'Deferred revenue', 'Balance Sheet', 'Deferred revenue'),
        ('us-gaap:ContractWithCustomerLiabilityCurrent', 'Deferred revenue', 'Balance Sheet', 'Deferred revenue'),
        ('us-gaap:DeferredRevenueNoncurrent', 'Deferred revenue non-current', 'Balance Sheet', 'Deferred revenue non-current'),

        -- Other
        ('us-gaap:ProceedsFromPaymentsForOtherFinancingActivities', 'Other financing activities', 'Cash Flow Statement', 'Other financing activities'),
        ('us-gaap:PaymentsForProceedsFromOtherInvestingActivities', 'Other investing activities', 'Cash Flow Statement', 'Other investing activities'),
        ('us-gaap:OtherNoncashIncomeExpense', 'Other income expenses', 'Cash Flow Statement', 'Other income expenses')
    """
    )

    # Create index on concept_normalizations for performance
    op.create_index(
        op.f("ix_concept_normalizations_concept"),
        "concept_normalizations",
        ["concept"],
        unique=False,
    )


def downgrade() -> None:
    # Drop indexes
    op.drop_index(
        op.f("ix_concept_normalizations_concept"), table_name="concept_normalizations"
    )

    # Drop concept_normalizations table
    op.drop_table("concept_normalizations")
