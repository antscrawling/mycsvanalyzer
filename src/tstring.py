from string.templatelib import Interpolation
from string import templatelib
from collections import Counter


def first_test():
    interpolation = t"{1. + 2.:.2f}".interpolations[0]
    #interpolation

    match interpolation:
        case Interpolation(value, expression, conversion, format_spec):
            print("First Test: ")
            print(value, expression, conversion, format_spec, sep=' | ')
        
"""
This is the t string: Template(strings=('Hello, ', '! Your data is ', '. Special character: ', ''), interpolations=(Interpolation('Alice', 'name', 's', ''), Interpolation([1, 2, 3], 'data', 'r', ''), Interpolation('你好', 'special_char', 'a', '')))
First element is Template
Second element is Special character
Third element is interpolations

"""        
        
"""The available conversion types are:
!s (string conversion): This is the default if no conversion is specified. It calls str() on the value.
!r (representation conversion): This calls repr() on the value, providing a developer-friendly representation.
!a (ASCII conversion): This calls ascii() on the value, which returns an ASCII-only representation, escaping non-ASCII characters.
"""

def second_test():
    from string import templatelib

    name = "Alice"
    data = [1, 2, 3]
    special_char = "你好" # Non-ASCII characters
    
    # Create a t-string
    temp  = t"Hello, {name!s}! Your data is {data!r}. Special character: {special_char!a}"
    print(f"The whole string is {temp}")
    print(f"for the test 2. This is the t string: {temp.interpolations[0]}")
    print(f"for the test 3. This is the t string: {temp.interpolations[1]}")
    print(f"for the test 4. This is the t string: {temp.interpolations[2]}")
    # Accessing the interpolations to see the conversion component
    

    #first t string
    print("Second Test: ")
    match temp.interpolations[0]:  # Accessing the third interpolation
        case Interpolation(value, expression, conversion, format_spec):
            print(value, expression, conversion, format_spec, sep=' | ')
    #second t string
    print("Third Test: ")
    match temp.interpolations[1]:  # Accessing the third interpolation
        case Interpolation(value, expression, conversion, format_spec):
            print(value, expression, conversion, format_spec, sep=' | ')
    #third t string
    print("Fourth Test: ")
    match temp.interpolations[2]:  # Accessing the third interpolation
        case Interpolation(value, expression, conversion, format_spec):
            print(value, expression, conversion, format_spec, sep=' | ')

def third_test():
    import json
    with open('src/params_to_save.json','r') as file:
        params : dict[str,str|int] = json.load(file)
    #print(params)
    
    #for k,v in params.items():
        #print(f"{k}===|===={v} (type: {type(v)})\n")
    my_value_a = params['VALUE_A']
    my_value_b = params['VALUE_B']
    my_value_c = params['VALUE_C']
    print(t"The value of {VALUE_A}")
    # Create a t-string using the parameters from the JSON file
        
        
        
        
            
      
def main():
    #first_test()
    #second_test()
    third_test()
    
if __name__ == "__main__":
    main()