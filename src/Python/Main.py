from OSPLink import *
from builtins import print

conn = PyrrhoConnect("Files=Temp;User=Fred")
conn.open()
try:
    conn.act("create table a(b date)")
except DatabaseError as e:
    print(e.message)
conn.act("insert into a values(current_date)")
com = conn.createCommand()
com.commandText = 'select * from a'
rdr = com.executeReader()
while rdr.read():
    print(rdr.val(0))
rdr.close()
print("Done")
