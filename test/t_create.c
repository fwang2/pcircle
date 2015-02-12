#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>

int main(int argc, char**argv) {
    if (argc != 3) {
        fprintf(stderr, "Usage: t_create num_of_files\n");
        exit(1);
    }
    unsigned long count = atol(argv[1]);
    unsigned int sizemax = atoi(argv[2]);
    char fname[100];
    fprintf(stdout, "Creating %lu number of files\n", count);
    for (int i=0; i < count; i++) {
        sprintf(fname, "%d", i);
        int fd = open(fname, O_RDWR|O_CREAT, 0666);
        if (fd < 0) {
            fprintf(stderr, "[%s] failed: %s\n", fname, strerror(errno));
            exit(1);
        }
        posix_fallocate(fd, 0, rand() % sizemax);
        close(fd);
    }
}


