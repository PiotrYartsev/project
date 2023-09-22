import threading
import queue

# Define a reusable multithreading function
def run_threads(thread_count, function, data):
    # Create a queue for the objects
    object_queue = queue.Queue()

    # Put all objects from the list into the queue
    for item in data:
        # Print for debugging purposes
        #print(item)
        object_queue.put(item)

    # Function to execute by each thread
    def thread_function(output_list):
        local_output = []
        while True:
            try:
                # Get an object from the queue
                args = object_queue.get_nowait()

                # Call the provided function with the object's arguments
                result = function(args)  # Pass the entire list as one argument

                # Append the result to the local output list
                local_output.append(result)
            except queue.Empty:
                # If the queue is empty, break out of the loop
                break

        # Append the local output to the shared output list
        output_list.extend(local_output)

    # Create and start N threads
    threads = []
    output_list = []
    for _ in range(thread_count):
        thread = threading.Thread(target=thread_function, args=(output_list,))
        thread.start()
        threads.append(thread)

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

    print("All threads have finished.")
    
    # Return the combined output list
    return output_list
