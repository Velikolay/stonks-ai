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
        sa.Column("description", sa.String(), nullable=True),
        sa.Column("parent_concept", sa.String(), nullable=True),
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
        INSERT INTO concept_normalization_overrides (concept, normalized_label, statement, is_abstract) VALUES
        -- Revenue concepts
        ('us-gaap:Revenues', 'Revenue', 'Income Statement', False),
        ('us-gaap:SalesRevenueNet', 'Revenue', 'Income Statement', False),
        ('us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax', 'Revenue', 'Income Statement', False),
        ('us-gaap:RevenueFromContractWithCustomerIncludingAssessedTax', 'Revenue', 'Income Statement', False),
        ('us-gaap:OperatingRevenues', 'Revenue', 'Income Statement', False),

        -- Cost of Revenue concepts
        ('us-gaap:CostOfGoodsAndServicesSold', 'Cost of Revenue', 'Income Statement', False),
        ('us-gaap:CostOfRevenue', 'Cost of Revenue', 'Income Statement', False),
        ('us-gaap:CostOfGoodsSold', 'Cost of Revenue', 'Income Statement', False),

        -- Gross Profit concepts
        ('us-gaap:GrossProfit', 'Gross Profit', 'Income Statement', False),
        ('us-gaap:GrossProfitLoss', 'Gross Profit', 'Income Statement', False),

        -- Operating Income concepts
        ('us-gaap:OperatingIncomeLoss', 'Operating Income', 'Income Statement', False),
        ('us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxes', 'Operating Income', 'Income Statement', False),

        -- Net Income concepts
        ('us-gaap:NetIncomeLoss', 'Net Income', 'Income Statement', False),
        ('us-gaap:ProfitLoss', 'Net Income', 'Income Statement', False),
        ('us-gaap:IncomeLossFromContinuingOperationsAfterIncomeTaxes', 'Net Income', 'Income Statement', False),

        -- Income Before Tax from Continuing Operations
        ('us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest', 'Income Before Tax from Continuing Operations', 'Income Statement', False),
        ('us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments', 'Income Before Tax from Continuing Operations', 'Income Statement', False),

        -- Cash and Cash Equivalents concepts
        ('us-gaap:CashAndCashEquivalentsAtCarryingValue', 'Cash and Cash Equivalents', 'Balance Sheet', False),
        ('us-gaap:Cash', 'Cash and Cash Equivalents', 'Balance Sheet', False),
        ('us-gaap:CashEquivalentsAtCarryingValue', 'Cash and Cash Equivalents', 'Balance Sheet', False),

        -- Total Assets concepts
        ('us-gaap:Assets', 'Total Assets', 'Balance Sheet', False),
        ('us-gaap:AssetsTotal', 'Total Assets', 'Balance Sheet', False),

        -- Total Liabilities concepts
        ('us-gaap:Liabilities', 'Total Liabilities', 'Balance Sheet', False),
        ('us-gaap:LiabilitiesTotal', 'Total Liabilities', 'Balance Sheet', False),

        -- Total Equity concepts
        ('us-gaap:StockholdersEquity', 'Total Equity', 'Balance Sheet', False),
        ('us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest', 'Total Equity', 'Balance Sheet', False),
        ('us-gaap:Equity', 'Total Equity', 'Balance Sheet', False),

        -- Operating Cash Flow concepts
        ('us-gaap:NetCashProvidedByUsedInOperatingActivities', 'Operating Cash Flow', 'Cash Flow Statement', False),
        ('us-gaap:NetCashProvidedByUsedInOperatingActivitiesContinuingOperations', 'Operating Cash Flow', 'Cash Flow Statement', False),

        -- Investing Cash Flow concepts
        ('us-gaap:NetCashProvidedByUsedInInvestingActivities', 'Investing Cash Flow', 'Cash Flow Statement', False),
        ('us-gaap:NetCashProvidedByUsedInInvestingActivitiesContinuingOperations', 'Investing Cash Flow', 'Cash Flow Statement', False),

        -- Financing Cash Flow concepts
        ('us-gaap:NetCashProvidedByUsedInFinancingActivities', 'Financing Cash Flow', 'Cash Flow Statement', False),
        ('us-gaap:NetCashProvidedByUsedInFinancingActivitiesContinuingOperations', 'Financing Cash Flow', 'Cash Flow Statement', False),

        -- Free Cash Flow concepts
        ('us-gaap:FreeCashFlow', 'Free Cash Flow', 'Cash Flow Statement', False),

        -- EPS concepts
        ('us-gaap:EarningsPerShareBasic', 'Basic EPS', 'Income Statement', False),
        ('us-gaap:EarningsPerShareDiluted', 'Diluted EPS', 'Income Statement', False),

        -- Cash Dividents Per Share concepts
        ('us-gaap:CommonStockDividendsPerShareDeclared', 'Cash Dividends Per Share', 'Income Statement', False),

        -- Debt concepts
        ('us-gaap:LongTermDebt', 'Long-term Debt', 'Balance Sheet', False),
        ('us-gaap:LongTermDebtNoncurrent', 'Long-term Debt', 'Balance Sheet', False),
        ('us-gaap:ShortTermBorrowings', 'Short-term Debt', 'Balance Sheet', False),
        ('us-gaap:ShortTermDebt', 'Short-term Debt', 'Balance Sheet', False),

        -- Current Marketable Securities concepts
        ('us-gaap:MarketableSecuritiesCurrent', 'Current Marketable Securities', 'Balance Sheet', False),
        ('us-gaap:AvailableForSaleSecuritiesCurrent', 'Current Marketable Securities', 'Balance Sheet', False),

        -- Non-current Marketable Securities concepts
        ('us-gaap:MarketableSecuritiesNoncurrent', 'Non-current Marketable Securities', 'Balance Sheet', False),
        ('us-gaap:AvailableForSaleSecuritiesNoncurrent', 'Non-current Marketable Securities', 'Balance Sheet', False),
        ('us-gaap:AvailableForSaleSecuritiesDebtSecuritiesNoncurrent', 'Non-current Marketable Securities', 'Balance Sheet', False),

        -- Deferred Revenue concepts
        ('us-gaap:DeferredRevenueCurrent', 'Deferred revenue', 'Balance Sheet', False),
        ('us-gaap:ContractWithCustomerLiabilityCurrent', 'Deferred revenue', 'Balance Sheet', False),
        ('us-gaap:DeferredRevenueNoncurrent', 'Deferred revenue non-current', 'Balance Sheet', False),

        -- Other
        ('us-gaap:ProceedsFromPaymentsForOtherFinancingActivities', 'Other financing activities', 'Cash Flow Statement', False),
        ('us-gaap:PaymentsForProceedsFromOtherInvestingActivities', 'Other investing activities', 'Cash Flow Statement', False),
        ('us-gaap:OtherNoncashIncomeExpense', 'Other income expenses', 'Cash Flow Statement', False)
    """
    )

    # Create index on concept_normalization_overrides for performance
    op.create_index(
        op.f("ix_concept_normalization_overrides_concept"),
        "concept_normalization_overrides",
        ["concept"],
        unique=False,
    )


def downgrade() -> None:
    # Drop indexes
    op.drop_index(
        op.f("ix_concept_normalization_overrides_concept"),
        table_name="concept_normalization_overrides",
    )

    # Drop concept_normalization_overrides table
    op.drop_table("concept_normalization_overrides")
