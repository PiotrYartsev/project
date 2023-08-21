import psutil
import time
import matplotlib.pyplot as plt

# Start tracking CPU and memory usage
cpu_usage = []
mem_usage = []
time_list = []
try:
    while True:
        # Get CPU and memory usage
        cpu_percent = psutil.cpu_percent()
        mem_percent = psutil.virtual_memory().percent

        # Add usage to list
        cpu_usage.append(cpu_percent)
        mem_usage.append(mem_percent)

        # Add time to list
        time_list.append(time.time())
        # Wait for 1 second
        time.sleep(1)

except KeyboardInterrupt:
    # User pressed Ctrl+C, break the loop
    pass

# Calculate memory usage
min_mem = min(mem_usage)
mem_usage = [x - min_mem for x in mem_usage]
total_mem = psutil.virtual_memory().total / (1024 ** 3)  # GB
mem_usage = [i * total_mem / 100 for i in mem_usage]

# Calculate CPU usage
#cpu_usage = [psutil.cpu_percent() for _ in cpu_usage]

# Calculate time
min_time = min(time_list)
time_list = [i - min_time for i in time_list]

# Print CPU and memory usage
print("CPU usage:", cpu_usage)
print("Memory usage:", mem_usage)
print("Time:", time_list)

# Plot memory usage
plt.plot(time_list, mem_usage)
plt.xlabel("Time (s)")
plt.ylabel("Memory usage (GB)")
plt.show()

# Plot CPU usage
plt.plot(time_list, cpu_usage)
plt.xlabel("Time (s)")
plt.ylabel("CPU usage (%)")
plt.show()