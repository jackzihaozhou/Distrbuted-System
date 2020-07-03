all: mylib.so server clean

server: server.c
	gcc  -o server server.c 

mylib.o: mylib.c
	gcc -Wall -fPIC -DPIC -c mylib.c -I../include 

mylib.so: mylib.o
	ld -shared -o mylib.so mylib.o -ldl -L../lib

.PHONY: clean

clean:
	rm -f *.o

