DEPSEC =
DEPFIG = fig/launch1.eps \
	fig/launch2.eps \
	fig/launch3.eps \
	fig/launch4.eps \
	fig/container.eps

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
