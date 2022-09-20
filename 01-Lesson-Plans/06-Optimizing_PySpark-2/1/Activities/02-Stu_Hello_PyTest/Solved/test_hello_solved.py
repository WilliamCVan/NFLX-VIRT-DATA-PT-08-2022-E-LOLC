def function_one(value: str):
    return_value = len(value) * 5
    return return_value

def function_two(value: str):
    return_value = value.replace('e', '')
    return return_value

def test_function_one():
    assert function_one("hello") == 25

def test_function_two():
    assert function_two("hello") == 'hllo'

