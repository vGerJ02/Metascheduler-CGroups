#include <stdio.h>
#include <stdlib.h>
#include <time.h>

long long sum_of_squares(long long n) {
    long long sum = 0;
    for (long long i = 1; i <= n; ++i) {
        sum += i * i;
    }
    return sum;
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        printf("Usage: %s <n>\n", argv[0]);
        return 1;
    }

    long long n = atoll(argv[1]);

    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);  // marca temps inicial

    long long result = sum_of_squares(n);

    clock_gettime(CLOCK_MONOTONIC, &end);    // marca temps final

    long long elapsed_sec = end.tv_sec - start.tv_sec;
    long long elapsed_nsec = end.tv_nsec - start.tv_nsec;
    double elapsed = elapsed_sec + elapsed_nsec / 1e9;

    printf("Sum of squares up to %lld: %lld\n", n, result);
    printf("Temps d'execució: %.6f segons\n", elapsed);

    return 0;
}
