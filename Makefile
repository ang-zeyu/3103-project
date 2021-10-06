build:
	gcc -fopenmp *.c -o proxy

build-opt:
	gcc -O2 -fopenmp *.c -o proxy

build-debug:
	gcc -O2 -g -fopenmp *.c -o proxy
	gdb proxy

run:
	./proxy 8080 1 blacklist.txt
