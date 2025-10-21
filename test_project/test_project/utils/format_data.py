from loguru import logger
from test_project.utils.find_maximum_number import find_maximum_number


def format_authors(authors_data):
    authors_dict = {}
    last_name = None

    for item in authors_data:
        item = item.strip()

        if all(ch.isdigit() or ch == "," for ch in item):
            nums = [int(num.strip()) for num in item.split(",") if num.strip()]
            if last_name:
                clean_name = last_name.lstrip(",. ").strip()
                for n in nums:
                    authors_dict.setdefault(n, []).append(clean_name)
            last_name = None
        else:
            last_name = item

    return authors_dict


def format_affiliations(data):
    affiliations_dictionary = {}
    max_num = find_maximum_number(data)

    for dictionary_index in range(1, max_num + 1):
        list_of_affiliations = []
        for index, value in enumerate(data):
            if value.strip() == str(dictionary_index):
                affiliation = (data[index + 1]).strip().rstrip(";")
                list_of_affiliations.append(affiliation)
        if list_of_affiliations:
            affiliations_dictionary[dictionary_index] = "; ".join(list_of_affiliations)
    return affiliations_dictionary


def format_names_and_affiliations_dictionary(d1, d2):
    result = {}

    for k, names in d1.items():
        if k in d2:
            for name in names:
                if name not in result:
                    result[name] = []
                result[name].append(d2[k])

    for name in result:
        result[name] = "; ".join(result[name])

    return result
