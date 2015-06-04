'''
Created on Nov 20, 2014

@author: mingchen7
'''

import subprocess

ppath = r'C:\Program Files\R\R-3.1.1\bin\x64\Rscript.exe'
r_file_name = 'helloworld.R'
# r_file_name = 'EMImputation.R'
proc = subprocess.Popen("%s %s" % (ppath, r_file_name), stdout=subprocess.PIPE)
#proc = subprocess.Popen([])
output = proc.stdout.read()

print output