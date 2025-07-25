#include <stdio.h>
#include <stdlib.h>

long long sum_of_squares(int n) {
    long long sum = 0;
    for (int i = 1; i <= n; ++i) {
        sum += (long long)i * i;
    }
    return sum;
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        printf("Usage: %s <n>\n", argv[0]);
        return 1;
    }
    int n = atoi(argv[1]);
    long long result = sum_of_squares(n);
    printf("Sum of squares up to %d: %lld\n", n, result);
    return 0;
}
