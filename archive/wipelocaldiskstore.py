import os
import os.path
import sys

if __name__ == '__main__':
   path = sys.argv[1]
   for fname in os.listdir(path):
      os.remove(os.path.join(path, fname))