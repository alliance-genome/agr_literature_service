from typing import List

from agr_literature_service.api.crud.workflow_tag_crud import g


def get_refs_to_convert() -> List[str]:
    pass


def convert_ref(reference_curie: str):
    pass


def main():
    for ref_curie in get_refs_to_convert():
        convert_ref(ref_curie)


if __name__ == '__main__':
    main()
