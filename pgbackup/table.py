from typing import Any, List, Optional

SPACE_AROUND = 2


def space_content(value: Any, max_space_to_fill: int, centered: bool):
    if centered:
        return str(value).center(max_space_to_fill + SPACE_AROUND)
    else:
        return f" {str(value).ljust(max_space_to_fill)}" + " "


def build_content(row: List[Any], max_sizes: List[int], centered: bool) -> str:
    line = "│"
    for index, value in enumerate(row):
        max_space_to_fill = max_sizes[index]
        line += space_content(value, max_space_to_fill, centered) + "│"
    return line


def build_horizontal_border(
    max_sizes: List[int], join_char: str, start_char: str, end_char: str
) -> str:
    nbr_columns = len(max_sizes)
    nbr_separators = nbr_columns + 1
    table_length = sum(max_sizes) + (SPACE_AROUND * nbr_columns) + nbr_separators
    # 2 is for the left and right edges of the table
    space_to_fill = table_length - 2
    border = start_char + "─" * space_to_fill + end_char

    # add the middle join if nbr_columns > 1
    if nbr_columns > 1:
        extra_chars = SPACE_AROUND + 1
        for index, ln in enumerate(max_sizes):
            # to skip the last border
            if index < (nbr_columns - 1):
                join_pos = sum(max_sizes[: index + 1]) + extra_chars
                border = border[:join_pos] + join_char + border[join_pos + 1 :]
                extra_chars += SPACE_AROUND + 1

    return border


def build_header(labels: List[Any], max_sizes: List[int], centered: bool) -> str:
    header = (
        build_horizontal_border(max_sizes, join_char="┬", start_char="┌", end_char="┐")
        + "\n"
        + build_content(row=labels, max_sizes=max_sizes, centered=centered)
    )

    return header


def make_table(
    rows: List[List[Any]], labels: Optional[List[Any]] = None, centered: bool = False
) -> str:
    """
    :param rows: 2D list containing objects that have a single-line representation (via `str`).
    All rows must be of the same length.
    :param labels: List containing the column labels. If present, the length must equal to that of each row.
    :param centered: If the items should be aligned to the center, else they are left aligned.
    :return: A table representing the rows passed in.
    """

    max_sizes = [len(str(el)) for el in labels]
    # for each item in a row, replace at its index its length value if
    # it is greater than the current value in max_sizes
    for row in rows:
        for index, el in enumerate(row):
            if max_sizes[index] < len(str(el)):
                max_sizes[index] = len(str(el))

    content = "\n".join([build_content(row, max_sizes, centered) for row in rows])

    bottom_border = "\n" + build_horizontal_border(
        max_sizes, join_char="┴", start_char="└", end_char="┘"
    )

    top_border = (
        build_horizontal_border(max_sizes, join_char="┼", start_char="├", end_char="┤")
        + "\n"
    )

    content = top_border + content + bottom_border
    content = build_header(labels, max_sizes, centered) + "\n" + content
    return content
