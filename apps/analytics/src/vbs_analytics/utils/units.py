from decimal import ROUND_HALF_UP, Decimal


def format_wei(amount_wei: int) -> Decimal:
    """
    Converts a wei value to a token amount
    """
    value = Decimal(amount_wei) / Decimal(10**18)
    return value.quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)
