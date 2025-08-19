"""Tests for the geography parser."""

from filings.parsers.geography_parser import GeographyParser


class TestGeographyParser:
    """Test cases for GeographyParser."""

    def setup_method(self):
        """Set up test fixtures."""
        self.parser = GeographyParser()

    def test_parse_geography_americas(self):
        """Test parsing Americas geographies."""
        test_cases = [
            ("Americas", "Americas"),
            ("AmericasExcludingUnitedStates", "Americas"),
            ("NorthAmerica", "Americas"),
            ("SouthAmerica", "Americas"),
            ("LatinAmerica", "Americas"),
        ]

        for input_text, expected_geography in test_cases:
            result = self.parser.parse_geography(input_text)
            assert result is not None
            assert result.geography == expected_geography
            assert result.original_text == input_text

    def test_parse_geography_europe(self):
        """Test parsing Europe geographies."""
        test_cases = [
            ("Europe", "Europe"),
            ("EMEA", "Europe"),
            ("European", "Europe"),
        ]

        for input_text, expected_geography in test_cases:
            result = self.parser.parse_geography(input_text)
            assert result is not None
            assert result.geography == expected_geography
            assert result.original_text == input_text

    def test_parse_geography_asia_pacific(self):
        """Test parsing Asia Pacific geographies."""
        test_cases = [
            ("AsiaPacific", "AsiaPacific"),
            ("APAC", "AsiaPacific"),
            ("Asia", "AsiaPacific"),
            ("RestOfAsiaPacific", "AsiaPacific"),
        ]

        for input_text, expected_geography in test_cases:
            result = self.parser.parse_geography(input_text)
            assert result is not None
            assert result.geography == expected_geography
            assert result.original_text == input_text

    def test_parse_geography_china(self):
        """Test parsing China geographies."""
        test_cases = [
            ("China", "China"),
            ("GreaterChina", "China"),
            ("MainlandChina", "China"),
        ]

        for input_text, expected_geography in test_cases:
            result = self.parser.parse_geography(input_text)
            assert result is not None
            assert result.geography == expected_geography
            assert result.original_text == input_text

    def test_parse_geography_japan(self):
        """Test parsing Japan geographies."""
        test_cases = [
            ("Japan", "Japan"),
            ("Japanese", "Japan"),
        ]

        for input_text, expected_geography in test_cases:
            result = self.parser.parse_geography(input_text)
            assert result is not None
            assert result.geography == expected_geography
            assert result.original_text == input_text

    def test_parse_geography_united_states(self):
        """Test parsing United States geographies."""
        test_cases = [
            ("UnitedStates", "UnitedStates"),
            ("USA", "UnitedStates"),
            ("America", "UnitedStates"),
        ]

        for input_text, expected_geography in test_cases:
            result = self.parser.parse_geography(input_text)
            assert result is not None
            assert result.geography == expected_geography
            assert result.original_text == input_text

    def test_parse_geography_non_us(self):
        """Test parsing Non-US geographies."""
        test_cases = [
            ("NonUS", "NonUS"),
            ("NonUnitedStates", "NonUS"),
        ]

        for input_text, expected_geography in test_cases:
            result = self.parser.parse_geography(input_text)
            assert result is not None
            assert result.geography == expected_geography
            assert result.original_text == input_text

    def test_parse_geography_other_geographies(self):
        """Test parsing other geographies."""
        test_cases = [
            ("MiddleEast", "MiddleEast"),
            ("Africa", "Africa"),
            ("African", "Africa"),
            ("India", "India"),
            ("Indian", "India"),
            ("Korea", "Korea"),
            ("SouthKorea", "Korea"),
        ]

        for input_text, expected_geography in test_cases:
            result = self.parser.parse_geography(input_text)
            assert result is not None
            assert result.geography == expected_geography
            assert result.original_text == input_text

    def test_parse_geography_case_insensitive(self):
        """Test that parsing is case insensitive."""
        test_cases = [
            ("americas", "Americas"),
            ("EUROPE", "Europe"),
            ("AsIaPaCiFiC", "AsiaPacific"),
            ("china", "China"),
            ("japan", "Japan"),
            ("unitedstates", "UnitedStates"),
            ("nonus", "NonUS"),
        ]

        for input_text, expected_geography in test_cases:
            result = self.parser.parse_geography(input_text)
            assert result is not None
            assert result.geography == expected_geography

    def test_parse_geography_no_match(self):
        """Test parsing text that doesn't match any geography."""
        test_cases = [
            "",
            "RandomText",
            "ProductRevenue",
            "CustomerSegment",
            "123456",
            "Revenue",
            "Sales",
        ]

        for input_text in test_cases:
            result = self.parser.parse_geography(input_text)
            assert result is None

    def test_parse_geography_invalid_input(self):
        """Test parsing with invalid input types."""
        test_cases = [
            None,
            123,
            [],
            {},
        ]

        for input_text in test_cases:
            result = self.parser.parse_geography(input_text)
            assert result is None

    def test_parse_geographies_multiple(self):
        """Test parsing multiple geographies at once."""
        input_texts = ["Americas", "Europe", "AsiaPacific", "China", "Japan"]
        results = self.parser.parse_geographies(input_texts)

        assert len(results) == 5
        geographies = [result.geography for result in results]
        assert "Americas" in geographies
        assert "Europe" in geographies
        assert "AsiaPacific" in geographies
        assert "China" in geographies
        assert "Japan" in geographies

    def test_parse_geographies_mixed(self):
        """Test parsing mixed valid and invalid geographies."""
        input_texts = ["Americas", "RandomText", "Europe", "InvalidGeography"]
        results = self.parser.parse_geographies(input_texts)

        assert len(results) == 2
        geographies = [result.geography for result in results]
        assert "Americas" in geographies
        assert "Europe" in geographies

    def test_is_geography_text(self):
        """Test the is_geography_text method."""
        # Valid geographies
        assert self.parser.is_geography_text("Americas") is True
        assert self.parser.is_geography_text("Europe") is True
        assert self.parser.is_geography_text("China") is True

        # Invalid geographies
        assert self.parser.is_geography_text("RandomText") is False
        assert self.parser.is_geography_text("") is False
        assert self.parser.is_geography_text(None) is False

    def test_get_known_geographies(self):
        """Test getting the list of known geographies."""
        geographies = self.parser.get_known_geographies()

        expected_geographies = {
            "Americas",
            "Europe",
            "AsiaPacific",
            "China",
            "Japan",
            "UnitedStates",
            "NonUS",
            "MiddleEast",
            "Africa",
            "India",
            "Korea",
        }

        assert geographies == expected_geographies
