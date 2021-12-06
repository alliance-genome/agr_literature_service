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
else:
    print("pytest successful. No FAILED found in output")
    exit(0)
