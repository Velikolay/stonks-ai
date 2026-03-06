CREATE OR REPLACE PROCEDURE refresh_financials(company_ids int[])
LANGUAGE plpgsql
AS $$
BEGIN
    IF company_ids IS NULL OR array_length(company_ids, 1) IS NULL THEN
        RETURN;
    END IF;

    CALL refresh_financial_facts_overridden(company_ids);
    CALL refresh_concept_normalization(company_ids);
    CALL refresh_hierarchy_normalization(company_ids);
    CALL refresh_dimension_normalization(company_ids);
    CALL refresh_financial_facts_normalized(company_ids);
    CALL refresh_quarterly_financials(company_ids);
    CALL refresh_yearly_financials(company_ids);
END;
$$;
