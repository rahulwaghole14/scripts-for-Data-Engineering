''' test_batch_deduplicate.py '''
from .batch_deduplicate import remove_duplicates

def test_remove_duplicates():
    '''test remove duplicates behave as expected'''
    bad_data = [
        {"id": 1, "meta": {"lastModified": "2022-01-01"}},
        {"id": 2, "meta": {"lastModified": "2022-01-02"}},
        {"id": 1, "meta": {"lastModified": "2022-01-01"}},
        {"id": 1, "meta": {"lastModified": "2022-01-02"}},
        {"id": 3, "meta": {"lastModified": "2022-01-03"}},
        {"id": 2, "meta": {"lastModified": "2022-01-02"}},
        {"id": 4},
        {"meta": {"lastModified": "2022-01-02"}},
    ]
    expected_result = [
        {"id": 1, "meta": {"lastModified": "2022-01-02"}},
        {"id": 2, "meta": {"lastModified": "2022-01-02"}},
        {"id": 3, "meta": {"lastModified": "2022-01-03"}},
        {"id": 4}
    ]
    assert remove_duplicates(bad_data) == expected_result

def test_remove_duplicates_empty():
    '''test remove duplicates behave as expected'''
    bad_data = []
    expected_result = []
    assert remove_duplicates(bad_data) == expected_result

def test_remove_duplicates_none():
    '''test remove duplicates behave as expected'''
    bad_data = None
    expected_result = []
    assert remove_duplicates(bad_data) == expected_result
