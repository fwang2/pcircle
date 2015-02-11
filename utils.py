import sys
import time
import itertools




# SO: http://stackoverflow.com/questions/13520622/python-script-to-show-progress
def spiner():
    for c in itertools.cycle('|/-\\'):
        sys.stdout.write('\r' + c)
        sys.stdout.flush()
        time.sleep(0.2)

# SO: http://stackoverflow.com/questions/3002085/python-to-print-out-status-bar-and-percentage

def progress():
    import sys
    total = 10000000
    point = total / 100
    increment = total / 20
    for i in xrange(total):
        if(i % (5 * point) == 0):
            sys.stdout.write("\r[" + "=" * (i / increment) +  " " * ((total - i)/ increment) + "]" +  str(i / point) + "%")
            sys.stdout.flush()
