import time as t
import datetime

#import specific class
from datetime import date

print(t.time())
print(datetime.date.today())

print(date.today())

print("Name of module is: ",__name__)

if __name__ == '__main__':
    print("Executed when invoked directly")
else:
    print("Executed when imported")