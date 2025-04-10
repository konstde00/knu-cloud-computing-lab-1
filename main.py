import json
import random
from Pyro4 import expose

class Solver:
    def __init__(self, workers=None, input_file_name=None, output_file_name=None):
        self.input_file_name = input_file_name
        self.output_file_name = output_file_name
        self.workers = workers
        self.matrix = None
        print("Inited")

    def solve(self):
        print("Job Started")
        if not self.workers:
            print("No workers found.")
            return
        print("Workers %d" % len(self.workers))

        n, m, matrix = self.read_input()
        self.matrix = matrix

        matrix_str = json.dumps(matrix)

        chunk_size = (m + len(self.workers) - 1) // len(self.workers)  # round up
        mapped = []

        for i in range(len(self.workers)):
            start_col = i * chunk_size
            end_col = min(m, (i+1) * chunk_size)
            if start_col >= end_col:
                break  # No more columns left

            print("map worker %d for columns [%d, %d)" % (i, start_col, end_col))
            mapped.append(
                self.workers[i].mymap(str(start_col), str(end_col), matrix_str)
            )

        transposed = self.myreduce(mapped)

        self.write_output(transposed)

        print("Job Finished")

    @staticmethod
    @expose
    def mymap(a, b, count):
        start_col = int(a)
        end_col = int(b)

        matrix = json.loads(count)
        n = len(matrix)
        if n == 0:
            return []

        submatrix = [row[start_col:end_col] for row in matrix]

        transposed_block = list(map(list, zip(*submatrix)))

        result = []
        for row in transposed_block:
            row_str = " ".join(map(str, row))
            result.append(row_str)

        return result

    @staticmethod
    @expose
    def myreduce(mapped):

        print("reduce")
        output = []
        for partial_result in mapped:
            print("reduce loop")
            output += partial_result.value
        print("reduce done")
        return output

    def read_input(self):

        with open(self.input_file_name, 'r') as f:
            n = int(f.readline())
            m = int(f.readline())
            matrix = []
            for _ in range(n):
                row = f.readline().strip().split()
                # Convert to int if you want an integer matrix.
                row = list(map(int, row))
                matrix.append(row)
        return n, m, matrix

    def write_output(self, transposed_rows):

        with open(self.output_file_name, 'w') as f:
            # Each row of the transposed matrix goes on its own line.
            # The items in transposed_rows are already space-separated strings.
            for row_str in transposed_rows:
                f.write(row_str + "\n")
        print("output done")

    @staticmethod
    @expose
    def is_probable_prime(n):

        assert n >= 2
        if n == 2:
            return True
        if n % 2 == 0:
            return False
        s = 0
        d = n - 1
        while True:
            quotient, remainder = divmod(d, 2)
            if remainder == 1:
                break
            s += 1
            d = quotient

        def try_composite(a):
            if pow(a, d, n) == 1:
                return False
            for i in range(s):
                if pow(a, 2 ** i * d, n) == n - 1:
                    return False
            return True

        a = random.randrange(2, n)
        return not try_composite(a)
