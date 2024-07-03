import random


def pair_names(names):
    random.shuffle(names)
    pairs = []
    for i in range(0, 6, 2):
        pairs.append((names[i], names[i+1]))
    return pairs


names = ["Harry", "Holly", "Harley", "Amreet", "Valentine", "David"]
pairs = pair_names(names)
print(pairs)

digits = [str(random.randint(0, 9)) for _ in range(16)]
random_number = ''.join(digits)
print(random_number)
