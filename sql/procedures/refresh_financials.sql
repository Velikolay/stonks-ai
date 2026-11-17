CREATE OR REPLACE PROCEDURE refresh_financials(company_ids int[])
LANGUAGE plpgsql
AS $$
DECLARE
    company int;
BEGIN
    IF company_ids IS NULL OR array_length(company_ids, 1) IS NULL THEN
        RETURN;
    END IF;

    -- One company at a time: stages never read across companies, and the
    -- plans grow faster than linearly with the number of companies.
    FOREACH company IN ARRAY company_ids LOOP
        CALL refresh_financial_facts_overridden(ARRAY[company]);
        CALL refresh_concept_normalization(ARRAY[company]);
        CALL refresh_hierarchy_normalization(ARRAY[company]);
        CALL refresh_dimension_normalization(ARRAY[company]);
        CALL refresh_financial_facts_normalized(ARRAY[company]);
        CALL refresh_dimension_breakdowns(ARRAY[company]);
        CALL refresh_quarterly_financials(ARRAY[company]);
        CALL refresh_yearly_financials(ARRAY[company]);
        -- The stages' ON COMMIT DROP tables outlive the call in one transaction.
        DROP TABLE IF EXISTS
            tmp_financial_facts_overridden_new,
            tmp_concept_normalization_new,
            tmp_hierarchy_normalization_new,
            tmp_dimension_normalization_new,
            tmp_financial_facts_normalized_new,
            tmp_breakdown_rows,
            tmp_dimension_breakdowns_new,
            tmp_quarterly_financials_new,
            tmp_yearly_financials_new;
    END LOOP;
END;
$$;
