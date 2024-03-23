from concurrent.futures import ThreadPoolExecutor
import threading

class Test:
    def __init__(self):
        self.num_threads = 5
        self.pros = [{"Arg1": "Test1", "Arg2": "Test11"}, {"Arg1": "Test2", "Arg2": "Test22"},
                     {"Arg1": "Test3", "Arg2": "Test33"}, {"Arg1": "Test4", "Arg2": "Test44"},
                     {"Arg1": "Test5", "Arg2": "Test55"}, {"Arg1": "Test6", "Arg2": "Test66"},
                     {"Arg1": "Test7", "Arg2": "Test77"}, {"Arg1": "Test8", "Arg2": "Test88"},
                     {"Arg1": "Test9", "Arg2": "Test99"}, {"Arg1": "Test10", "Arg2": "Test1010"}]
        self.status = []
        self.exception_occurred = False  # Flag to track exceptions

    def execute_parallel(self, proc):
        try:
            print(f"Inserting into table values ('{proc['Arg1']}', '{proc['Arg2']}')")
            # Simulating an exception for demonstration
            if proc['Arg1'] == 'Test9':
                raise Exception("Simulated Exception")
            return {"status": "success", "args": proc}
        except Exception as e:
            # Capture essential information from the exception
            exception_info = str(e)[:100]  # Limiting to first 100 characters
            self.exception_occurred = True
            return {"status": "failure", "args": proc, "exception": exception_info}

    def main(self):
        with ThreadPoolExecutor(max_workers=self.num_threads) as pl:
            results = pl.map(self.execute_parallel, self.pros)
            for result in results:
                self.status.append(result)

        # Print status of each thread
        for idx, result in enumerate(self.status):
            print(f"Thread {idx + 1}: {result['status']}")
            if result['status'] == 'failure':
                print(f"Exception: {result['exception']}")
            print(f"Arguments processed: {result['args']}")
            print()

        # Check if any exceptions occurred
        if self.exception_occurred:
            raise Exception("At least one thread encountered an exception")

if __name__ == '__main__':
    obj = Test()
    obj.main()
