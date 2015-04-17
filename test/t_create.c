#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>

int sizemax = 100;

void gen_random(char *s, const int len) {
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    for (int i = 0; i < len; ++i) {
        s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }

    s[len] = 0;
}

int main(int argc, char**argv) {
    if (argc != 2) {
        fprintf(stderr, "Usage: t_create num_of_files\n");
        exit(1);
    }
    unsigned long count = atol(argv[1]);
    char fname[100];
    char buf[100];
    int len = 0;

    fprintf(stdout, "Creating %lu number of files\n", count);
    for (int i=0; i < count; i++) {
        sprintf(fname, "f%09d", i);
        int fd = open(fname, O_RDWR|O_CREAT, 0666);
        if (fd < 0) {
            fprintf(stderr, "[%s] failed: %s\n", fname, strerror(errno));
            exit(1);
        }
        len = rand() % sizemax;
        gen_random(buf, len);
        buf[len] = '\0';
        write(fd, buf, len);
        // posix_fallocate(fd, 0, rand() % sizemax);
        close(fd);

        if (i % 100 == 0) {
            double percent = (double) i / count;
            fprintf(stderr, "%.2f are done\n", percent);
        }
    }
}


