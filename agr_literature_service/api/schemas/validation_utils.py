def validate_non_empty(v: str, field: str) -> str:
    """Strip surrounding whitespace and reject empty/whitespace-only values.

    Shared by Pydantic field validators for required free-text string fields.
    """
    if not v or not v.strip():
        raise ValueError(f"{field} cannot be empty or whitespace")
    return v.strip()
