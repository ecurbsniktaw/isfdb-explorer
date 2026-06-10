import subprocess

# Runs awk and captures its output directly into a variable
result = subprocess.check_output("awk '{print $1}' data.txt", shell=True, text=True)
print(result)


# wc -l /var/log/nginx/access.log

result = subprocess.check_output("wc -l /var/log/nginx/access.log", shell=True, text=True)
print(f'# lines in log file = {result}')
