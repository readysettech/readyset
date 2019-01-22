HTML = instances.html graph.html nodes.html node.html

all: $(HTML)

%.html: %.tmpl %.js.tmpl
	python gen.py $* > $*.html

clean:
	rm $(HTML)
