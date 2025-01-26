def process(s: str):
    yield (s,)
    yield (s.upper(),)


x = process("abcd")
for val in x:
    print(val)