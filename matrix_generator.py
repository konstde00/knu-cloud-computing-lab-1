import random

def generate_matrix(rows, cols, filename):
    with open(filename, 'w') as f:
        for _ in range(rows):
            row_data = [str(random.randint(0, 1000)) for _ in range(cols)]
            f.write(' '.join(row_data) + '\n')

generate_matrix(1000, 1000, 'input.txt')
