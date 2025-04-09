from Pyro4 import expose

class MatrixMultiplier:
    def __init__(self, workers=None, A_file=None, B_file=None, C_file=None):
        self.workers = workers
        self.A_file = A_file
        self.B_file = B_file
        self.C_file = C_file

    def solve(self):
        # 1. Зчитуємо матриці A та B
        A = self.read_matrix(self.A_file)   # Розмір n x m
        B = self.read_matrix(self.B_file)   # Розмір m x k
        n = len(A)
        if not self.workers or len(self.workers) == 0:
            raise ValueError("Необхідно передати хоча б одного воркера")
        chunk_size = n // len(self.workers)
        partial_results = []

        # 2. Розподіл рядків A між воркерами (map-фаза)
        for i, worker in enumerate(self.workers):
            start = i * chunk_size
            end = (i + 1) * chunk_size if i != len(self.workers) - 1 else n
            A_chunk = A[start:end]

            # Виклик віддаленого методу mymap для обчислення частини матриці
            result = worker.mymap(A_chunk, B, start_index=start)
            partial_results.append(result)

        # 3. Збирання результатів (reduce-фаза)
        C = self.myreduce(partial_results, n, len(B[0]))

        self.write_matrix(self.C_file, C)
        print(f"Результат успішно записаний у файл: {self.C_file}")

    @staticmethod
    @expose
    def mymap(A_chunk, B, start_index=0):
        """
        Обчислює частину результатної матриці для заданого блоку рядків A (A_chunk),
        множачи їх на матрицю B. Повертає список кортежів (global_row_index, row_result).
        """
        m = len(B)       # Кількість рядків у матриці B
        k = len(B[0])    # Кількість стовпців у матриці B
        partial_C = []

        for i, rowA in enumerate(A_chunk):
            new_row = []
            for col_idx in range(k):
                s = 0
                for r in range(m):
                    s += rowA[r] * B[r][col_idx]
                new_row.append(s)
            partial_C.append((start_index + i, new_row))
        return partial_C

    @staticmethod
    @expose
    def myreduce(mapped_results, n, k):
        """
        Об'єднує результати обчислень, отримані від різних воркерів, у єдину матрицю C розміру n x k.
        """
        C = [[0] * k for _ in range(n)]
        for part in mapped_results:
            # Якщо повернутий об'єкт має атрибут .value (з Pyro4 Proxy), отримуємо його,
            # інакше працюємо безпосередньо з part.
            try:
                part_list = part.value
            except AttributeError:
                part_list = part
            for row_idx, row_values in part_list:
                C[row_idx] = row_values
        return C

    def read_matrix(self, filename):
        """
        Зчитує матрицю з файлу. Кожен рядок у файлі містить цілих чисел,
        розділених пробілами.
        """
        matrix = []
        with open(filename, 'r') as f:
            for line in f:
                if line.strip():
                    row = list(map(int, line.strip().split()))
                    matrix.append(row)
        return matrix

    def write_matrix(self, filename, matrix):
        """
        Записує матрицю у файл. Перший рядок файлу містить розміри матриці,
        наступні рядки – елементи матриці.
        """
        with open(filename, 'w') as f:
            if matrix:
                n, k = len(matrix), len(matrix[0])
                f.write(f"{n} {k}\n")
            for row in matrix:
                f.write(' '.join(map(str, row)) + '\n')
