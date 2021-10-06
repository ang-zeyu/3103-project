build:
	gcc -O2 -fopenmp *.c -o main

run:
	./main 8080 1 blacklist.txt
