#!/usr/bin/python
import pystache
import sys

def usage():
    print("usage: gen.py <page>")

if len(sys.argv) < 2:
    usage()
    sys.exit(1)

page = sys.argv[1]

context = {}
context['{}_ACTIVE'.format(page)] = 'active';
context["NAVBAR"] = pystache.render(open("navbar.tmpl").read(), context)
context["MAIN"] = pystache.render(open("{}.tmpl".format(page)).read())
context["PAGE_JS"] = pystache.render(open("{}.js.tmpl".format(page)).read())

template = open("top.tmpl").read()
print(pystache.render(template, context))
