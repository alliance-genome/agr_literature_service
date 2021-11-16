with open('pytest.out') as f:
    lines = f.readlines()

okay = True
for line in lines:
    if 'FAILED' in line:
        okay = False

if not okay:
    for line in lines:
        print(line)
    exit(-1)
exit(0)
