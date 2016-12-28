#!python
#
# Copyright (c) 2015-2016 Pivotal Software, Inc. All Rights Reserved.
#
import jinja2
import jinja2.filters
import json
import os.path
import sys

def renderTemplate(directory, templateFile, context):
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(directory))
    env.trim_blocks = True
    env.lstrip_blocks = True
    outputFile = templateFile[:-4]
    template = env.get_template(templateFile)
    template.stream(context).dump(sys.stdout)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        sys.exit("must provide the name of a context file and a template file in the current directory")
        
    templateDir = '.'
    templateFileName = sys.argv[2]
    contextFileName = sys.argv[1]
    
    with open(contextFileName, 'r') as contextFile:
        context = json.load(contextFile)
        
    renderTemplate(templateDir,templateFileName,context)
    
    
