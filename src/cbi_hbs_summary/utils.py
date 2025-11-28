import polars as pl


INVISIBLE_CHARS = [
    chr(8203),
    chr(173),
    chr(8207),
    chr(8235),
    chr(8236),
    chr(8234),
    chr(65279)
]


UNWANTED_SYMBOLS = [
    "\n",
    "\r",
    "\t",
    "…",
    "ـ",
    "_",
    "•",
    "\\*",
    "`",
    "\"",
    "\'",
    "«",
    "»",
    ",",
    ";",
    ":",
]


def sanitize_farsi_text(column: pl.Expr) -> pl.Expr:
    """
    Clean Farsi text by replacing Arabic characters, removing invisible and unwanted characters,
    normalizing spaces, and stripping leading/trailing spaces.
    """

    return (
        column
        .pipe(replace_arabic_characters)

        # Replace Zero Width Non-Joiner ('\u200c') with a space
        .str.replace_all(chr(8204), " ")

        # Remove other invisible and unwanted characters
        .str.replace_all(
            "[" + "".join(INVISIBLE_CHARS + UNWANTED_SYMBOLS) + "]", ""
        )

        # Normalize spaces: replace all multi-space occurrences with a single space
        .str.replace_all("\\s+", " ")

        .str.strip_chars()

        .str.replace(r"\.0$", "")
    )


def replace_arabic_characters(column: pl.Expr) -> pl.Expr:
    """
    Replace Arabic characters with their Farsi equivalents in the Series.
    """
    character_mapping = {
        chr(1610): chr(1740), # ي -> ی
        chr(1574): chr(1740), # ئ -> ی
        chr(1609): chr(1740), # ى -> ی
        chr(1571): chr(1575), # أ -> ا
        chr(1573): chr(1575), # إ -> ا
        chr(1572): chr(1608), # ؤ -> و
        chr(1603): chr(1705), # ك -> ک
        chr(1728): chr(1607), # ۀ -> ه
        chr(1577): chr(1607), # ة -> ه
    }
    return column.str.replace_many(character_mapping)
