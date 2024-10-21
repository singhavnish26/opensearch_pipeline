b = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i']
for item in b:
    print(item)

def split_list(biglist, shard_size):
    # Using list comprehension to create sublists of the given size
    return [biglist[i:i + shard_size] for i in range(0, len(biglist), shard_size)]

sublists = split_list(b, 3)
print(sublists)
