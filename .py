__PI = '314159265358979323846264338327950288419716939937510582097494'
SQRT = '141421356237309504880168872420969807856967187537694807317667'

x = SQRT  # PI
# print(x[:60], len(x))

for i in range(6):
    print(f'{__PI[i*15:(i+1)*15]}')

for i in range(6):
    print(f'{SQRT[i*15:(i+1)*15]}')