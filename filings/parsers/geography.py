"""
Geography parser for extracting geographic information from business segment strings.
"""

import re
from dataclasses import dataclass
from typing import List, Optional, Set


@dataclass
class GeographyInfo:
    """Information about a detected geography."""

    geography: str
    original_text: str


class GeographyParser:
    """
    Parser for extracting geographic regions from business segment strings.

    Handles various region formats like:
    - Americas, AmericasExcludingUnitedStates
    - Europe, EMEA
    - China, GreaterChina
    - Japan, AsiaPacific, RestOfAsiaPacific
    - US, NonUs
    """

    def __init__(self):
        # Define geography patterns with their variations
        self.geography_patterns = {
            # Americas
            "Americas": [
                r"americas(?:excludingunitedstates)?",
                r"northamerica",
                r"southamerica",
                r"latinamerica",
            ],
            # Europe
            "Europe": [
                r"europe",
                r"emea",  # Europe, Middle East, and Africa
                r"european",
            ],
            # Asia Pacific
            "AsiaPacific": [
                r"asiapacific",
                r"apac",
                r"asia",
                r"restofasiapacific",
            ],
            # China
            "China": [
                r"china",
                r"greaterchina",
                r"mainlandchina",
            ],
            # Japan
            "Japan": [
                r"japan",
                r"japanese",
            ],
            # United States
            "UnitedStates": [
                r"unitedstates",
                r"usa",
                r"america",
            ],
            # Non-US
            "NonUS": [
                r"nonus",
                r"nonunitedstates",
            ],
            # Other regions
            "MiddleEast": [
                r"middleeast",
            ],
            "Africa": [
                r"africa",
                r"african",
            ],
            "India": [
                r"india",
                r"indian",
            ],
            "Korea": [
                r"korea",
                r"southkorea",
            ],
        }

        # Compile regex patterns for efficiency
        self.compiled_patterns = {}
        for geography, patterns in self.geography_patterns.items():
            self.compiled_patterns[geography] = [
                re.compile(pattern, re.IGNORECASE) for pattern in patterns
            ]

    def parse_geography(self, text: str) -> Optional[GeographyInfo]:
        """
        Parse a single text string to extract geography information.

        Args:
            text: The text string to parse (e.g., "AmericasExcludingUnitedStates")

        Returns:
            GeographyInfo if a geography is detected, None otherwise
        """
        if not text or not isinstance(text, str):
            return None

        # Clean the text (remove extra whitespace, convert to lowercase)
        cleaned_text = text.strip().lower()

        best_match = None
        longest_match_length = 0

        for geography, patterns in self.compiled_patterns.items():
            for pattern in patterns:
                match = pattern.search(cleaned_text)
                if match:
                    match_length = len(match.group())
                    if match_length > longest_match_length:
                        longest_match_length = match_length
                        best_match = GeographyInfo(
                            geography=geography, original_text=text
                        )

        return best_match

    def parse_geographies(self, texts: List[str]) -> List[GeographyInfo]:
        """
        Parse multiple text strings to extract geography information.

        Args:
            texts: List of text strings to parse

        Returns:
            List of GeographyInfo objects for detected geographies
        """
        results = []
        for text in texts:
            geography_info = self.parse_geography(text)
            if geography_info:
                results.append(geography_info)
        return results

    def get_known_geographies(self) -> Set[str]:
        """
        Get the set of all known geography names.

        Returns:
            Set of geography names
        """
        return set(self.geography_patterns.keys())

    def is_geography_text(self, text: str) -> bool:
        """
        Check if a text string likely contains geography information.

        Args:
            text: Text to check

        Returns:
            True if text likely contains geography information
        """
        return self.parse_geography(text) is not None
