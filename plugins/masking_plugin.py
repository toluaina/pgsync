"""Plugin for masking sensitive data before indexing."""

import hashlib
import re
import typing as t

from pgsync import plugin


class MaskingPlugin(plugin.Plugin):
    """
    Mask or redact sensitive data in documents before indexing.

    This plugin provides PII (Personally Identifiable Information) masking
    for common sensitive data patterns like emails, phone numbers, SSNs,
    and credit card numbers.

    Customize the FIELDS_TO_MASK and patterns to match your data model.
    """

    name: str = "Masking"

    # Fields to completely redact (replace with "[REDACTED]")
    REDACT_FIELDS: t.List[str] = ["ssn", "password", "secret"]

    # Fields to mask (show partial data)
    MASK_FIELDS: t.List[str] = ["email", "phone", "credit_card"]

    # Fields to hash (one-way anonymization)
    HASH_FIELDS: t.List[str] = ["user_id", "customer_id"]

    # Regex patterns for auto-detection
    PATTERNS = {
        "email": re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"),
        "phone": re.compile(r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b"),
        "ssn": re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
        "credit_card": re.compile(
            r"\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b"
        ),
    }

    def mask_email(self, email: str) -> str:
        """Mask email showing only first char and domain."""
        if "@" not in email:
            return email
        local, domain = email.rsplit("@", 1)
        if len(local) > 1:
            return f"{local[0]}***@{domain}"
        return f"***@{domain}"

    def mask_phone(self, phone: str) -> str:
        """Mask phone showing only last 4 digits."""
        digits = re.sub(r"\D", "", phone)
        if len(digits) >= 4:
            return f"***-***-{digits[-4:]}"
        return "***"

    def mask_credit_card(self, cc: str) -> str:
        """Mask credit card showing only last 4 digits."""
        digits = re.sub(r"\D", "", cc)
        if len(digits) >= 4:
            return f"****-****-****-{digits[-4:]}"
        return "****"

    def hash_value(self, value: str) -> str:
        """One-way hash for anonymization."""
        return hashlib.sha256(value.encode()).hexdigest()[:16]

    def transform(self, doc: dict, **kwargs) -> dict:
        """Apply masking rules to document fields."""
        if not doc:
            return doc

        for field in self.REDACT_FIELDS:
            if field in doc:
                doc[field] = "[REDACTED]"

        for field in self.MASK_FIELDS:
            if field in doc and isinstance(doc[field], str):
                value = doc[field]
                if "email" in field.lower():
                    doc[field] = self.mask_email(value)
                elif "phone" in field.lower():
                    doc[field] = self.mask_phone(value)
                elif "credit" in field.lower() or "card" in field.lower():
                    doc[field] = self.mask_credit_card(value)

        for field in self.HASH_FIELDS:
            if field in doc and doc[field]:
                doc[field] = self.hash_value(str(doc[field]))

        return doc
