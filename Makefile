all: instances.html graph.html

%.html: %.tmpl %.js.tmpl
	python gen.py $* > $*.html

clean:
	rm *.html
