#define _XOPEN_SOURCE 700
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <time.h>

/*
 * Monte Carlo estimation of Pi (serial version).
 *
 * Throw N random points into the unit square [0,1)x[0,1).
 * Count how many land inside the quarter-circle of radius 1.
 * Pi ~ 4 * (inside / N).
 *
 * This is embarrassingly parallel: each sample is independent.
 */
void monte_carlo_pi(long num_samples) {
    long inside = 0;

    srand48(time(NULL));

    for (long i = 0; i < num_samples; i++) {
        double x = drand48();
        double y = drand48();
        if (x * x + y * y <= 1.0) {
            inside++;
        }
    }

    double pi_estimate = 4.0 * (double)inside / (double)num_samples;

    printf("Samples:    %ld\n", num_samples);
    printf("Inside:     %ld\n", inside);
    printf("Pi estimate: %.10f\n", pi_estimate);
    printf("Error:       %.10f\n", fabs(pi_estimate - M_PI));
}

/*
 * Mandelbrot set generation (serial version).
 *
 * For each pixel (px, py) in a width x height grid, map to complex
 * coordinates and iterate z = z^2 + c up to max_iter times.
 * Store the iteration count in a flat array.
 *
 * Embarrassingly parallel: each pixel is independent.
 */
void mandelbrot(int width, int height, int max_iter) {
    int *iters = (int *)malloc((size_t)width * height * sizeof(int));

    double x_min = -2.0, x_max = 1.0;
    double y_min = -1.5, y_max = 1.5;

    long total_inside = 0;

    for (int py = 0; py < height; py++) {
        for (int px = 0; px < width; px++) {
            double cr = x_min + (x_max - x_min) * px / width;
            double ci = y_min + (y_max - y_min) * py / height;

            double zr = 0.0, zi = 0.0;
            int iter = 0;
            while (zr * zr + zi * zi <= 4.0 && iter < max_iter) {
                double tmp = zr * zr - zi * zi + cr;
                zi = 2.0 * zr * zi + ci;
                zr = tmp;
                iter++;
            }

            iters[py * width + px] = iter;
            if (iter == max_iter) {
                total_inside++;
            }
        }
    }

    printf("Mandelbrot %dx%d, max_iter=%d\n", width, height, max_iter);
    printf("Pixels in set: %ld / %ld\n", total_inside, (long)width * height);

    free(iters);
}

/*
 * Numerical integration using the midpoint rule (serial version).
 *
 * Integrates f(x) = sin(x)*log(x+1) + sqrt(x) over [a, b] by dividing
 * the interval into N equal sub-intervals and evaluating f at each midpoint.
 *
 * Embarrassingly parallel: each sub-interval evaluation is independent.
 */
static double integrate_f(double x) {
    return sin(x) * log(x + 1.0) + sqrt(x);
}

void numerical_integration(long num_intervals, double a, double b) {
    double h = (b - a) / num_intervals;
    double sum = 0.0;

    for (long i = 0; i < num_intervals; i++) {
        double mid = a + (i + 0.5) * h;
        sum += integrate_f(mid);
    }

    double result = sum * h;

    printf("Integration of f(x) over [%.2f, %.2f]\n", a, b);
    printf("Intervals:  %ld\n", num_intervals);
    printf("Result:     %.10f\n", result);
}

int main(int argc, char *argv[]) {

    if (argc < 5) {
        printf("Usage: main_original <num_samples> <mandel_size> <max_iter> <num_intervals>\n");
        return 1;
    }

    long num_samples    = atol(argv[1]);
    int mandel_size     = atoi(argv[2]);
    int max_iter        = atoi(argv[3]);
    long num_intervals  = atol(argv[4]);

    monte_carlo_pi(num_samples);
    mandelbrot(mandel_size, mandel_size, max_iter);
    numerical_integration(num_intervals, 0.0, 10.0);

    return 0;
}
