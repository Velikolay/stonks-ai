"""add_concept_normalization

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
        ('us-gaap_Revenues', 'Revenue', 'Income Statement', 'Total revenue'),
        ('us-gaap_SalesRevenueNet', 'Revenue', 'Income Statement', 'Net sales revenue'),
        ('us-gaap_RevenueFromContractWithCustomerExcludingAssessedTax', 'Revenue', 'Income Statement', 'Revenue from contracts with customers'),
        ('us-gaap_RevenueFromContractWithCustomerIncludingAssessedTax', 'Revenue', 'Income Statement', 'Revenue from contracts including tax'),
        ('us-gaap_OperatingRevenues', 'Revenue', 'Income Statement', 'Operating revenues'),

        -- Cost of Revenue concepts
        ('us-gaap_CostOfGoodsAndServicesSold', 'Cost of Revenue', 'Income Statement', 'Cost of goods and services sold'),
        ('us-gaap_CostOfRevenue', 'Cost of Revenue', 'Income Statement', 'Cost of revenue'),
        ('us-gaap_CostOfGoodsSold', 'Cost of Revenue', 'Income Statement', 'Cost of goods sold'),

        -- Gross Profit concepts
        ('us-gaap_GrossProfit', 'Gross Profit', 'Income Statement', 'Gross profit'),
        ('us-gaap_GrossProfitLoss', 'Gross Profit', 'Income Statement', 'Gross profit or loss'),

        -- Operating Income concepts
        ('us-gaap_OperatingIncomeLoss', 'Operating Income', 'Income Statement', 'Operating income or loss'),
        ('us-gaap_IncomeLossFromContinuingOperationsBeforeIncomeTaxes', 'Operating Income', 'Income Statement', 'Income from continuing operations before taxes'),

        -- Net Income concepts
        ('us-gaap_NetIncomeLoss', 'Net Income', 'Income Statement', 'Net income or loss'),
        ('us-gaap_ProfitLoss', 'Net Income', 'Income Statement', 'Profit or loss'),
        ('us-gaap_IncomeLossFromContinuingOperationsAfterIncomeTaxes', 'Net Income', 'Income Statement', 'Income from continuing operations after taxes'),

        -- Cash and Cash Equivalents concepts
        ('us-gaap_CashAndCashEquivalentsAtCarryingValue', 'Cash and Cash Equivalents', 'Balance Sheet', 'Cash and cash equivalents'),
        ('us-gaap_Cash', 'Cash and Cash Equivalents', 'Balance Sheet', 'Cash'),
        ('us-gaap_CashEquivalentsAtCarryingValue', 'Cash and Cash Equivalents', 'Balance Sheet', 'Cash equivalents'),

        -- Total Assets concepts
        ('us-gaap_Assets', 'Total Assets', 'Balance Sheet', 'Total assets'),
        ('us-gaap_AssetsTotal', 'Total Assets', 'Balance Sheet', 'Total assets'),

        -- Total Liabilities concepts
        ('us-gaap_Liabilities', 'Total Liabilities', 'Balance Sheet', 'Total liabilities'),
        ('us-gaap_LiabilitiesTotal', 'Total Liabilities', 'Balance Sheet', 'Total liabilities'),

        -- Total Equity concepts
        ('us-gaap_StockholdersEquity', 'Total Equity', 'Balance Sheet', 'Stockholders equity'),
        ('us-gaap_StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest', 'Total Equity', 'Balance Sheet', 'Total stockholders equity'),
        ('us-gaap_Equity', 'Total Equity', 'Balance Sheet', 'Total equity'),

        -- Operating Cash Flow concepts
        ('us-gaap_NetCashProvidedByUsedInOperatingActivities', 'Operating Cash Flow', 'Cash Flow Statement', 'Net cash from operating activities'),
        ('us-gaap_NetCashProvidedByUsedInOperatingActivitiesContinuingOperations', 'Operating Cash Flow', 'Cash Flow Statement', 'Net cash from operating activities - continuing'),

        -- Investing Cash Flow concepts
        ('us-gaap_NetCashProvidedByUsedInInvestingActivities', 'Investing Cash Flow', 'Cash Flow Statement', 'Net cash from investing activities'),
        ('us-gaap_NetCashProvidedByUsedInInvestingActivitiesContinuingOperations', 'Investing Cash Flow', 'Cash Flow Statement', 'Net cash from investing activities - continuing'),

        -- Financing Cash Flow concepts
        ('us-gaap_NetCashProvidedByUsedInFinancingActivities', 'Financing Cash Flow', 'Cash Flow Statement', 'Net cash from financing activities'),
        ('us-gaap_NetCashProvidedByUsedInFinancingActivitiesContinuingOperations', 'Financing Cash Flow', 'Cash Flow Statement', 'Net cash from financing activities - continuing'),

        -- Free Cash Flow concepts
        ('us-gaap_FreeCashFlow', 'Free Cash Flow', 'Cash Flow Statement', 'Free cash flow'),

        -- EPS concepts
        ('us-gaap_EarningsPerShareBasic', 'Basic EPS', 'Income Statement', 'Basic earnings per share'),
        ('us-gaap_EarningsPerShareDiluted', 'Diluted EPS', 'Income Statement', 'Diluted earnings per share'),

        -- Debt concepts
        ('us-gaap_LongTermDebt', 'Long-term Debt', 'Balance Sheet', 'Long-term debt'),
        ('us-gaap_LongTermDebtNoncurrent', 'Long-term Debt', 'Balance Sheet', 'Long-term debt noncurrent'),
        ('us-gaap_ShortTermBorrowings', 'Short-term Debt', 'Balance Sheet', 'Short-term borrowings'),
        ('us-gaap_ShortTermDebt', 'Short-term Debt', 'Balance Sheet', 'Short-term debt'),

        -- Deferred Revenue concepts
        ('us-gaap_DeferredRevenueCurrent', 'Deferred revenue', 'Balance Sheet', 'Deferred revenue'),
        ('us-gaap_ContractWithCustomerLiabilityCurrent', 'Deferred revenue', 'Balance Sheet', 'Deferred revenue'),
        ('us-gaap_DeferredRevenueNoncurrent', 'Deferred revenue non-current', 'Balance Sheet', 'Deferred revenue non-current')
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
