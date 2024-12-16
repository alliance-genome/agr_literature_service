import unicodedata
from typing import List, Dict


class Author:
    def __init__(self, name, first_name, last_name, first_initial, order, orcid, affiliations: List[str],
                 first_author=None, corresponding_author=None, string_affiliations: str = ""):
        self.name = name
        self.first_name = first_name
        self.last_name = last_name
        self.first_initial = first_initial
        self.order = order
        self.orcid = orcid
        self.affiliations = affiliations
        self.first_author = first_author
        self.corresponding_author = corresponding_author
        self.string_affiliations = string_affiliations

    @staticmethod
    def normalize_field(field, set_lowercase: bool = False):
        if isinstance(field, str):
            normalized_field = field.strip() if field else ''
            if set_lowercase:
                normalized_field = normalized_field.lower()
        elif isinstance(field, list):
            normalized_field = '|'.join(sub_field.strip() for sub_field in field) if field else ''
            if set_lowercase:
                normalized_field = normalized_field.lower()
        elif isinstance(field, int):
            return field
        else:
            return field
            # raise Exception("Unsupported field type")
        return normalized_field

    def get_normalized_author(self, set_lowercase: bool = False):
        return Author(self.normalize_field(self.name, set_lowercase),
                      self.normalize_field(self.first_name, set_lowercase),
                      self.normalize_field(self.last_name, set_lowercase),
                      self.normalize_field(self.first_initial, set_lowercase),
                      self.order,
                      self.orcid,
                      self.affiliations,
                      self.first_author, self.corresponding_author,
                      self.normalize_field(self.string_affiliations, set_lowercase))

    def get_unique_key_based_on_names(self):
        """
        generate a unique key for each author based on specific attributes
        using last_name, first_name, first_initial, and name (fullname).
        """
        normalized_author = self.get_normalized_author(set_lowercase=True)
        return (normalized_author.last_name, normalized_author.first_name, normalized_author.first_initial,
                normalized_author.name)

    @staticmethod
    def get_unaccented_string(s):
        """
        normalize a string by removing accents.
        in Unicode normalization, 'NFKD' stands for "Normalization Form KD".
        """
        if s:
            normalized = unicodedata.normalize('NFKD', s)
            combined = ''.join([c for c in normalized if not unicodedata.combining(c)])
            return Author.normalize_field(combined)
        return None

    def get_key_based_on_unaccented_names(self):  # pragma: no cover
        name = self.get_unaccented_string(self.name)
        last_name = self.get_unaccented_string(self.last_name)
        first_initial = self.get_unaccented_string(self.first_initial)

        if last_name and first_initial:
            return f"{last_name} {first_initial[0]}".upper()

        first_name_parts = None
        if name:
            if "," in name:
                # "Andersson,M." | " Specchio,N.A."
                last_name, first_name_parts = name.replace(', ', ',').split(',')
            else:
                parts = name.split(' ')
                if len(parts) >= 2:
                    last_name = parts[-1]
                    first_name_parts = parts[:-1]
                    if len(last_name) < 3 and last_name.isupper() and \
                            len(first_name_parts) == 1 and len(first_name_parts[0]) > len(last_name) and \
                            not first_name_parts[0].isupper():
                        # example name: "Smith W", "Li H", " Blaha A", "Bahrami AH"
                        first_initial = last_name[0]
                        last_name = first_name_parts[0]
                        return f"{last_name} {first_initial}".upper()
        if last_name and first_name_parts:
            first_initial = ''.join([part[0] for part in first_name_parts if part])
            return f"{last_name} {first_initial[0]}".upper()
        else:
            return name.upper() if name else ''

    def fix_orcid_format(self):
        if self.orcid and isinstance(self.orcid, str):
            self.orcid = f"ORCID:{self.orcid}" if not self.orcid.upper().startswith(
                'ORCID') else self.orcid.upper()
        else:
            self.orcid = None

    @staticmethod
    def load_from_json_dict(x):
        loaded_author = Author(name=x['name'],
                               first_name=x['firstname'] if 'firstname' in x else x.get('firstName', ''),
                               last_name=x['lastname'] if 'lastname' in x else x.get('lastName', ''),
                               first_initial=x['firstinit'] if 'firstinit' in x else x.get('firstInit', ''),
                               order=x['authorRank'] if 'authorRank' in x else None,
                               affiliations=x['affiliations'] if x.get('affiliations') else [],
                               orcid=x['orcid'] if 'orcid' in x else None)
        normalized_author = loaded_author.get_normalized_author(set_lowercase=False)
        normalized_author.fix_orcid_format()
        return normalized_author

    @staticmethod
    def load_from_db_dict(x):
        loaded_author = Author(name=x['name'],
                               first_name=x.get('first_name', ''),
                               last_name=x.get('last_name', ''),
                               first_initial=x.get('first_initial', ''),
                               order=x.get('order'),
                               affiliations=x.get('affiliations', []),
                               orcid=x.get('orcid', None))
        normalized_author = loaded_author.get_normalized_author(set_lowercase=False)
        normalized_author.fix_orcid_format()
        return normalized_author

    @staticmethod
    def load_list_of_authors_from_json_dict_list(json_dict_list: List[Dict]):
        if json_dict_list is None or len(json_dict_list) == 0:
            return []
        authors = [Author.load_from_json_dict(json_dict) for json_dict in json_dict_list]
        add_order_to_list_of_authors(authors)
        return authors

    @staticmethod
    def load_list_of_authors_from_db_dict_list(db_dict_list: List[Dict]):
        if db_dict_list is None or len(db_dict_list) == 0:
            return []
        authors = [Author.load_from_db_dict(db_dict) for db_dict in db_dict_list]
        return authors

    def get_normalized_lowercase_author_string(self):
        normalized_author = self.get_normalized_author(set_lowercase=True)
        return (str(normalized_author.name) + " | " + str(normalized_author.first_name) + " | "
                + str(normalized_author.last_name) + " | " + str(normalized_author.first_initial) + " | "
                + str(normalized_author.orcid) + " | " + str(normalized_author.order) + " | "
                + str(normalized_author.string_affiliations))


def add_order_to_list_of_authors(authors: List[Author]):
    if len(authors) > 0 and authors[0].order is None:
        author_order = 0
        for author in authors:
            author_order += 1
            author.order = author_order


def authors_lists_are_equal(author_list1: List[Author], author_list2: List[Author]):
    return [author.get_normalized_lowercase_author_string() for author in author_list1] == [
        author.get_normalized_lowercase_author_string() for author in author_list2]


def authors_have_same_name(author1: Author, author2: Author):
    author1_normalized = author1.get_normalized_author(set_lowercase=True)
    author2_normalized = author2.get_normalized_author(set_lowercase=True)
    for word_name1 in author1_normalized.name.split(' '):
        for word_name2 in author2_normalized.name.split(' '):
            if (Author.get_unaccented_string(word_name1) == Author.get_unaccented_string(word_name2)
                    and len(word_name1) >= 4):
                return True
    return False
