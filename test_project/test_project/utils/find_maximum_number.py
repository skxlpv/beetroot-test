def find_maximum_number(data):
    max_number = 0

    for item in data:
        stripped = item.strip()
        if stripped.isdigit():
            number = int(stripped)
            if number > max_number:
                max_number = number
    return max_number