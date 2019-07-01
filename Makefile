HTML = domain.html instances.html graph.html nodes.html node.html
AUX_TMPL = navbar.tmpl top.tmpl

all: $(HTML)

%.html: %.tmpl %.js.tmpl $(AUX_TMPL)
	python3 gen.py $* > $*.html

clean:
	rm $(HTML)
