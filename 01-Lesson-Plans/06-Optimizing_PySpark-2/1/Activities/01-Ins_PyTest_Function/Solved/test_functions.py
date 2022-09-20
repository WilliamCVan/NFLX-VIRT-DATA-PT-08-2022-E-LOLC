# Function to be tested
def add_value(int_value):
    return int_value + 10

# First test function that will assert that add_value(5) equals 15, this will pass
def test_add_value_passing():
    assert add_value(5) == 15

# Second test function that will assert that add_value(5) equals 10, this will fail
def test_add_value_not_passing():
    assert add_value(5) != 10