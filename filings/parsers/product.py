"""
Product parser for extracting product information from business segment strings.
"""

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class ProductInfo:
    """Information about a detected product."""

    product: str
    original_text: str


class ProductParser:
    """
    Parser for extracting product information from business segment strings.

    Processes strings by:
    1. Stripping everything before a colon (including the colon)
    2. Removing "Member" at the end
    """

    def __init__(self):
        pass

    def parse_product(self, text: str) -> Optional[ProductInfo]:
        """
        Parse a single text string to extract product information.

        Args:
            text: The text string to parse (e.g., "aapl:IPhoneMember")

        Returns:
            ProductInfo if a product is detected, None otherwise
        """
        if not text or not isinstance(text, str):
            return None

        # Clean the text (remove extra whitespace)
        cleaned_text = text.strip()

        # Find the colon and strip everything before it (including the colon)
        colon_index = cleaned_text.find(":")
        if colon_index != -1:
            # Strip everything before and including the colon
            product_text = cleaned_text[colon_index + 1 :].strip()
        else:
            # No colon found, use the original text
            product_text = cleaned_text

        # Remove "Member" at the end (case-insensitive)
        if product_text.lower().endswith("member"):
            product_text = product_text[:-6].strip()

        # Return None if the result is empty
        if not product_text:
            return None

        return ProductInfo(product=product_text, original_text=text)

    def parse_products(self, texts: List[str]) -> List[ProductInfo]:
        """
        Parse multiple text strings to extract product information.

        Args:
            texts: List of text strings to parse

        Returns:
            List of ProductInfo objects for detected products
        """
        results = []
        for text in texts:
            product_info = self.parse_product(text)
            if product_info:
                results.append(product_info)
        return results

    def is_product_text(self, text: str) -> bool:
        """
        Check if a text string likely contains product information.

        Args:
            text: Text to check

        Returns:
            True if text likely contains product information
        """
        return self.parse_product(text) is not None
