HTML = domain.html instances.html graph.html nodes.html node.html
AUX_JS = js/connect.js js/formatters.js js/layout-worker.js
AUX_TMPL = navbar.tmpl top.tmpl

all: $(HTML)

%.html: %.tmpl %.js.tmpl $(AUX_TMPL) $(AUX_JS)
	python3 gen.py $* > $*.html

clean:
	rm $(HTML)
