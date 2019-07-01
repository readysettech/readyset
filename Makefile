HTML = instances.html graph.html nodes.html node.html

all: $(HTML)

%.html: %.tmpl %.js.tmpl
	python3 gen.py $* > $*.html

clean:
	rm $(HTML)
