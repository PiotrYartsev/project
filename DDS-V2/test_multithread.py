import threading
import queue

# Your function f that takes arguments x and y
def f(x, y):
    # Replace this with your actual function implementation
    print(f"Processing x={x}, y={y}")

# Define a reusable multithreading function
def run_threads(thread_count, function, data):
    # Create a queue for the objects
    object_queue = queue.Queue()

    # Put all objects from the list into the queue
    for item in data:
        object_queue.put(item)

    # Function to execute by each thread
    def thread_function():
        while True:
            try:
                # Get an object from the queue
                args = object_queue.get_nowait()

                # Call the provided function with the object's arguments
                function(*args)
            except queue.Empty:
                # If the queue is empty, break out of the loop
                break

    # Create and start N threads
    threads = []
    for _ in range(thread_count):
        thread = threading.Thread(target=thread_function)
        thread.start()
        threads.append(thread)

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

    print("All threads have finished.")

# List of objects L
L = [(1, 2), (3, 4), (5, 6), (7, 8), (9, 10)]  # Replace with your list

# Number of threads N
N = 3  # Replace with the desired number of threads

# Use the reusable multithreading function with your function f and data L
run_threads(N, f, L)
