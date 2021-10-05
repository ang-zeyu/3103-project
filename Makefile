build:
	gcc -fopenmp *.c -o main

run:
	./main 8080 1 blacklist.txt
