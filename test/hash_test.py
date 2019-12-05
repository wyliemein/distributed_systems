data = [ 10, 100, 32, 45, 58, 126, 3, 29, 200, 400, 0 ]

def shift_right(value, shift_value):
    """Shift right that allows for negative values, which shift left
    (Python shift operator doesn't allow negative shift values)"""
    if shift_value == None:
        return 0
    if shift_value < 0:
        return value << (-shift_value)
    else:
        return value >> shift_value

def find_hash():
    def hashf(val, i, j = None, k = None):
        return (shift_right(val, i) ^ shift_right(val, j) ^ shift_right(val, k)) & 0xF

    for i in range(-7, 8):
        for j in range(i, 8):
            #for k in xrange(j, 8):
                #j = None
                k = None
                outputs = set()
                for val in data:
                    hash_val = hashf(val, i, j, k)
                    if hash_val >= 11:
                        pass
                        #break
                    if hash_val in outputs:
                        break
                    else:
                        outputs.add(hash_val)
                else:
                    print(i, j, k, outputs)

if __name__ == '__main__':
    find_hash()