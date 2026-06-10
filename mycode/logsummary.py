import subprocess

# Runs awk and captures its output directly into a variable
# result = subprocess.check_output("awk '{print $1}' data.txt", shell=True, text=True)
# print(result)


# wc -l /var/log/nginx/access.log

num_lines  = subprocess.check_output("wc -l /var/log/nginx/access.log", shell=True, text=True)
time_first = subprocess.check_output("head -n 1 /var/log/nginx/access.log | awk '{print $4}'", shell=True, text=True)
time_last  = subprocess.check_output("tail -n 1 /var/log/nginx/access.log | awk '{print $4}'", shell=True, text=True)


print(' ')
print(f'# lines in log file = {num_lines}')
print(f'first date/time = {time_first}')
print(f'last date/time = {time_last}')


