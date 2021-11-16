import pytest
#from helper_file_processing import load_ref_xref, split_identifier, generate_cross_references_file

#from generate_dqm_json_test_set import load_sample_json

bob = 1


@pytest.fixture(autouse=True)
def run_around_tests():
    print("Need to run loader")
    assert 1 == bob
    yield

    print("After yield")
    assert 1 == bob


def test_another_test():
    #load_sample_json("BOB")
    assert 1 == bob


def test_order_of_tests():
    assert 0 == bob
