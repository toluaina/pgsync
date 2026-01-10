"""Plugin for generating URL-friendly slugs from text fields."""

import re
import typing as t
import unicodedata

from pgsync import plugin


class SlugPlugin(plugin.Plugin):
    """
    Generate URL-friendly slugs from document fields.

    Converts text fields into lowercase, hyphenated slugs suitable for URLs.
    Handles unicode characters, removes special characters, and normalizes
    whitespace.

    Example: "Hello World! This is a Test" -> "hello-world-this-is-a-test"
    """

    name: str = "Slug"

    # Source field to generate slug from
    SOURCE_FIELD: str = "title"

    # Target field to store the slug
    TARGET_FIELD: str = "slug"

    # Maximum slug length (0 = no limit)
    MAX_LENGTH: int = 100

    def slugify(self, text: str) -> str:
        """
        Convert text to a URL-friendly slug.

        Args:
            text: The text to convert

        Returns:
            A lowercase, hyphenated slug
        """
        if not text:
            return ""

        # Normalize unicode characters (é -> e, ñ -> n, etc.)
        text = unicodedata.normalize("NFKD", text)
        text = text.encode("ascii", "ignore").decode("ascii")

        # Convert to lowercase
        text = text.lower()

        # Replace spaces and underscores with hyphens
        text = re.sub(r"[\s_]+", "-", text)

        # Remove all non-alphanumeric characters except hyphens
        text = re.sub(r"[^a-z0-9-]", "", text)

        # Collapse multiple hyphens into one
        text = re.sub(r"-+", "-", text)

        # Remove leading/trailing hyphens
        text = text.strip("-")

        # Truncate to max length (at word boundary if possible)
        if self.MAX_LENGTH > 0 and len(text) > self.MAX_LENGTH:
            text = text[: self.MAX_LENGTH]
            # Try to break at last hyphen
            if "-" in text:
                text = text.rsplit("-", 1)[0]

        return text

    def transform(self, doc: dict, **kwargs) -> dict:
        """Generate slug from source field and add to document."""
        if not doc:
            return doc

        source_value = doc.get(self.SOURCE_FIELD)
        if source_value and isinstance(source_value, str):
            doc[self.TARGET_FIELD] = self.slugify(source_value)

        return doc


class MultiFieldSlugPlugin(plugin.Plugin):
    """
    Generate slugs from multiple fields.

    Combines multiple fields into a single slug, useful for creating
    unique identifiers from composite data.

    Example: title="My Post", date="2024-01-15" -> "my-post-2024-01-15"
    """

    name: str = "MultiFieldSlug"

    # Fields to combine for slug generation (in order)
    SOURCE_FIELDS: t.List[str] = ["title", "id"]

    # Separator between field values
    SEPARATOR: str = "-"

    # Target field to store the slug
    TARGET_FIELD: str = "slug"

    def slugify(self, text: str) -> str:
        """Convert text to a URL-friendly slug."""
        if not text:
            return ""
        text = unicodedata.normalize("NFKD", text)
        text = text.encode("ascii", "ignore").decode("ascii")
        text = text.lower()
        text = re.sub(r"[\s_]+", "-", text)
        text = re.sub(r"[^a-z0-9-]", "", text)
        text = re.sub(r"-+", "-", text)
        return text.strip("-")

    def transform(self, doc: dict, **kwargs) -> dict:
        """Generate slug from multiple source fields."""
        if not doc:
            return doc

        parts = []
        for field in self.SOURCE_FIELDS:
            value = doc.get(field)
            if value:
                parts.append(self.slugify(str(value)))

        if parts:
            doc[self.TARGET_FIELD] = self.SEPARATOR.join(parts)

        return doc
