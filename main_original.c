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

/*
 * Ray tracing a scene of spheres (serial version).
 *
 * Casts one ray per pixel through a simple scene containing several
 * spheres on a ground plane. Each ray is traced independently:
 *   - Find the closest sphere intersection
 *   - Compute diffuse (Lambertian) shading from a single point light
 *   - Store the resulting RGB value
 *
 * Embarrassingly parallel: each pixel's ray is independent.
 */

typedef struct { double x, y, z; } Vec3;

static Vec3 vec3(double x, double y, double z) {
    Vec3 v = {x, y, z}; return v;
}
static Vec3 vec3_add(Vec3 a, Vec3 b) {
    return vec3(a.x+b.x, a.y+b.y, a.z+b.z);
}
static Vec3 vec3_sub(Vec3 a, Vec3 b) {
    return vec3(a.x-b.x, a.y-b.y, a.z-b.z);
}
static Vec3 vec3_scale(Vec3 a, double s) {
    return vec3(a.x*s, a.y*s, a.z*s);
}
static double vec3_dot(Vec3 a, Vec3 b) {
    return a.x*b.x + a.y*b.y + a.z*b.z;
}
static Vec3 vec3_norm(Vec3 a) {
    double len = sqrt(vec3_dot(a, a));
    return vec3_scale(a, 1.0 / len);
}

typedef struct {
    Vec3 center;
    double radius;
    Vec3 color;
} Sphere;

static int sphere_intersect(Sphere *s, Vec3 origin, Vec3 dir, double *t) {
    Vec3 oc = vec3_sub(origin, s->center);
    double b = vec3_dot(oc, dir);
    double c = vec3_dot(oc, oc) - s->radius * s->radius;
    double disc = b * b - c;
    if (disc < 0) return 0;
    double sq = sqrt(disc);
    double t0 = -b - sq;
    double t1 = -b + sq;
    if (t0 > 0.001) { *t = t0; return 1; }
    if (t1 > 0.001) { *t = t1; return 1; }
    return 0;
}

void ray_trace(int width, int height) {
    unsigned char *image = (unsigned char *)malloc((size_t)width * height * 3);

    Sphere spheres[4];
    spheres[0] = (Sphere){vec3( 0.0,  0.0, 5.0), 1.0, vec3(1.0, 0.2, 0.2)};
    spheres[1] = (Sphere){vec3( 2.0,  0.0, 6.0), 1.0, vec3(0.2, 1.0, 0.2)};
    spheres[2] = (Sphere){vec3(-2.0,  0.0, 4.0), 1.0, vec3(0.2, 0.2, 1.0)};
    spheres[3] = (Sphere){vec3( 0.0, -101.0, 5.0), 100.0, vec3(0.8, 0.8, 0.8)};
    int num_spheres = 4;

    Vec3 light = vec3(5.0, 5.0, -2.0);
    Vec3 cam_origin = vec3(0.0, 1.0, -3.0);
    double fov = 1.0;

    long total_hits = 0;

    for (int py = 0; py < height; py++) {
        for (int px = 0; px < width; px++) {
            double u = (2.0 * px / width - 1.0) * ((double)width / height);
            double v = 1.0 - 2.0 * py / height;
            Vec3 dir = vec3_norm(vec3(u * fov, v * fov, 1.0));

            double closest_t = 1e20;
            int hit_idx = -1;

            for (int s = 0; s < num_spheres; s++) {
                double t;
                if (sphere_intersect(&spheres[s], cam_origin, dir, &t)) {
                    if (t < closest_t) {
                        closest_t = t;
                        hit_idx = s;
                    }
                }
            }

            Vec3 pixel_color;
            if (hit_idx >= 0) {
                total_hits++;
                Vec3 hit_point = vec3_add(cam_origin, vec3_scale(dir, closest_t));
                Vec3 normal = vec3_norm(vec3_sub(hit_point, spheres[hit_idx].center));
                Vec3 to_light = vec3_norm(vec3_sub(light, hit_point));
                double diffuse = vec3_dot(normal, to_light);
                if (diffuse < 0.0) diffuse = 0.0;
                double ambient = 0.1;
                double brightness = ambient + 0.9 * diffuse;
                pixel_color = vec3_scale(spheres[hit_idx].color, brightness);
            } else {
                double t_sky = 0.5 * (v + 1.0);
                pixel_color = vec3_add(vec3_scale(vec3(1.0, 1.0, 1.0), 1.0 - t_sky),
                                       vec3_scale(vec3(0.5, 0.7, 1.0), t_sky));
            }

            int idx = (py * width + px) * 3;
            image[idx + 0] = (unsigned char)(fmin(pixel_color.x, 1.0) * 255);
            image[idx + 1] = (unsigned char)(fmin(pixel_color.y, 1.0) * 255);
            image[idx + 2] = (unsigned char)(fmin(pixel_color.z, 1.0) * 255);
        }
    }

    printf("Ray trace %dx%d, %d spheres\n", width, height, num_spheres);
    printf("Pixels hit: %ld / %ld\n", total_hits, (long)width * height);

    free(image);
}

/*
 * Brute-force hash cracking (serial version).
 *
 * Uses DJB2 hash as a stand-in. A target string is hashed, then all
 * candidate strings of a given length over a charset are enumerated
 * and hashed until a match is found.
 *
 * Embarrassingly parallel: each candidate hash is independent.
 */

static unsigned long djb2_hash(const char *str, int len) {
    unsigned long hash = 5381;
    for (int i = 0; i < len; i++) {
        hash = ((hash << 5) + hash) + (unsigned char)str[i];
    }
    return hash;
}

void brute_force_crack(int password_len) {
    const char charset[] = "abcdefghijklmnopqrstuvwxyz";
    int charset_size = 26;

    /* Build a target password from the middle of the charset */
    char target[16];
    for (int i = 0; i < password_len; i++) {
        target[i] = charset[(i * 7 + 3) % charset_size];
    }
    target[password_len] = '\0';
    unsigned long target_hash = djb2_hash(target, password_len);

    printf("Hash crack: target='%s' hash=%lu len=%d\n", target, target_hash, password_len);

    /* Total candidates = charset_size ^ password_len */
    long total = 1;
    for (int i = 0; i < password_len; i++) total *= charset_size;

    char candidate[16];
    candidate[password_len] = '\0';
    long found_at = -1;
    long checked = 0;

    for (long idx = 0; idx < total; idx++) {
        /* Convert idx to a candidate string (base-26 encoding) */
        long tmp = idx;
        for (int pos = password_len - 1; pos >= 0; pos--) {
            candidate[pos] = charset[tmp % charset_size];
            tmp /= charset_size;
        }

        checked++;
        if (djb2_hash(candidate, password_len) == target_hash) {
            found_at = idx;
            break;
        }
    }

    if (found_at >= 0) {
        printf("Found:      '%s' at index %ld\n", candidate, found_at);
    } else {
        printf("Not found after %ld candidates\n", checked);
    }
    printf("Checked:    %ld / %ld\n", checked, total);
}

int main(int argc, char *argv[]) {

    if (argc < 7) {
        printf("Usage: main_original <num_samples> <mandel_size> <max_iter> <num_intervals> <rt_size> <pw_len>\n");
        return 1;
    }

    long num_samples    = atol(argv[1]);
    int mandel_size     = atoi(argv[2]);
    int max_iter        = atoi(argv[3]);
    long num_intervals  = atol(argv[4]);
    int rt_size         = atoi(argv[5]);
    int pw_len          = atoi(argv[6]);

    monte_carlo_pi(num_samples);
    mandelbrot(mandel_size, mandel_size, max_iter);
    numerical_integration(num_intervals, 0.0, 10.0);
    ray_trace(rt_size, rt_size);
    brute_force_crack(pw_len);

    return 0;
}
