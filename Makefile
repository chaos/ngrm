SUBDIRS = foreign pepe zmq-broker pmi

all clean:
	for f in $(SUBDIRS); do make -C $$f $@; done
