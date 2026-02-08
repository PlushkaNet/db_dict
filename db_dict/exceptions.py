"""
There are two types of errors.
First (UnserializableError) is for objects that cannot be serialized
(like dicts/lists with classes/functions).
And second (UnsupportedType) is for directly passed objects/functions (unzerializble objects)
"""
class UnserializableError(Exception):
    def __init__(self, text):
        super().__init__(text)

class UnsupportedType(Exception):
    def __init__(self, text):
        super().__init__(text)