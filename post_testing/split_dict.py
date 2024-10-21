"""
Allows to split a dictionary into chunks of equal size. If the length of the dictionary is not divisible by the chunk
size, the last chunk will be smaller. For example, if the length of the dictionary is 10 and the chunk size is 4,
then we'll have three chunks: two of size 4 and one of size 2.

The first function returns a list with the dictionaries and the second one creates a generator.
"""


def split_dictionary(input_dict, chunk_size):
    res = []
    new_dict = {}
    for k, v in input_dict.items():
        if len(new_dict) < chunk_size:
            new_dict[k] = v
        else:
            res.append(new_dict)
            new_dict = {k: v}
    res.append(new_dict)
    return res


def gen_split_dictionary(input_dict, chunk_size):
    new_dict = {}
    for k, v in input_dict.items():
        if len(new_dict) < chunk_size:
            new_dict[k] = v
        else:
            yield new_dict
            new_dict = {k: v}
    yield