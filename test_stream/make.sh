gcc -g -c -Wall -Werror -fpic -o produce.o produce.c
gcc -g -shared -o produce.so produce.o

gcc -g -c -Wall -Werror -o consume.o consume.c
gcc -g -o consume consume.o
