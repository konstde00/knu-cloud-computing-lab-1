import random

def generate_matrix(rows, cols, filename):
    with open(filename, 'w') as f:
        for _ in range(rows):
            row_data = [str(random.randint(0, 1000)) for _ in range(cols)]
            f.write(' '.join(row_data) + '\n')

# Приклад створення 500×500 матриці:
generate_matrix(1000, 1000, 'A.txt')
generate_matrix(1000, 1000, 'B.txt')
