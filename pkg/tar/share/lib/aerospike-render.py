#/usr/bin/python
'''
SYNOPSIS

  python aerospike-render.py <TEMPLATE> [<NAME>=<VALUE> [...]]

DESCRIPTION

  Render the template file specified by <TEMPLATE> using the variables 
  defined by the -D option. 

  An example of defining the 'home' variable:

      -D home=/opt/aerospike

  The 'home' variable may be used in the <TEMPLATE> by specifying it as:

      ${home}

'''

import sys
from string import Template

if len(sys.argv) < 1 :
  sys.stderr.write("error: Missing required template file.\n")
  sys.stderr.write("\n")
  sys.stderr.write("%s\n" % __doc__.lstrip())
  sys.exit(1)

# template file name
templateFile = sys.argv[1]

# create dict of variables
variables = dict([ tuple(pair.split("=")) for pair in sys.argv[2:]])

# read the template and process it
sys.stdout.write(Template(open(templateFile).read()).substitute(variables))