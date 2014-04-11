DEPSEC =
DEPFIG = fig/launch1.pdf \
	fig/launch2.pdf \
	fig/launch3.pdf \
	fig/launch4.pdf \
	fig/container.pdf

DEPBIB = \
	../bib/project.bib \
	../bib/rfc.bib

all: report.pdf

view: all
	evince report.pdf

report.pdf: report.tex

clean:
	rm -f *.aux *.log *.dvi *.pdf *.blg *.bbl
	rm -f fig/*.pdf fig/*.eps

report.pdf: report.tex $(DEPSEC) $(DEPFIG) $(DEPBIB)
	rm -f *.log *.aux *.blg *.bbl
	pdflatex -shell-escape report
	bibtex report
	pdflatex -shell-escape report
	pdflatex -shell-escape report

%.eps: %.dia
	dia --nosplash --export=$@ --filter=eps-builtin $<

%.pdf: %.eps
	rm -f $@
	pdflatex -shell-escape report
