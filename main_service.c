#define _XOPEN_SOURCE 700
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <time.h>
#include <thpool.h>
#include <pthread.h>
#include <stdatomic.h>
#include <string.h>

// Global threadpool for parallel execution
static threadpool global_thpool = NULL;
static int num_threads = 4;

// Initialize global threadpool
void init_global_threadpool() {
    if (global_thpool == NULL) {
        global_thpool = thpool_init(num_threads);
    }
}

// Cleanup global threadpool
void cleanup_global_threadpool() {
    if (global_thpool != NULL) {
        thpool_destroy(global_thpool);
        global_thpool = NULL;
    }
}

/*
 * Monte Carlo estimation of Pi.
 * Split samples across threads, each with thread-local RNG and count.
 */
typedef struct {
    long start;
    long end;
    unsigned short seed[3];
    long inside;
} MCChunk;

static void mc_worker(void *arg) {
    MCChunk *c = (MCChunk *)arg;
    long inside = 0;
    for (long i = c->start; i < c->end; i++) {
        double x = erand48(c->seed);
        double y = erand48(c->seed);
        if (x * x + y * y <= 1.0)
            inside++;
    }
    c->inside = inside;
}

void monte_carlo_pi(long num_samples) {
    MCChunk *chunks = (MCChunk *)malloc(num_threads * sizeof(MCChunk));
    long per_thread = num_samples / num_threads;

    for (int i = 0; i < num_threads; i++) {
        chunks[i].start = i * per_thread;
        chunks[i].end = (i == num_threads - 1) ? num_samples : (i + 1) * per_thread;
        chunks[i].inside = 0;
        unsigned int s = (unsigned int)time(NULL) ^ (unsigned int)(i * 1337);
        chunks[i].seed[0] = (unsigned short)(s & 0xFFFF);
        chunks[i].seed[1] = (unsigned short)((s >> 16) & 0xFFFF);
        chunks[i].seed[2] = (unsigned short)(i + 1);
    }

    for (int i = 0; i < num_threads; i++)
        thpool_add_work(global_thpool, mc_worker, &chunks[i]);
    thpool_wait(global_thpool);

    long inside = 0;
    for (int i = 0; i < num_threads; i++)
        inside += chunks[i].inside;

    double pi_estimate = 4.0 * (double)inside / (double)num_samples;

    printf("Samples:    %ld\n", num_samples);
    printf("Inside:     %ld\n", inside);
    printf("Pi estimate: %.10f\n", pi_estimate);
    printf("Error:       %.10f\n", fabs(pi_estimate - M_PI));

    free(chunks);
}

/*
 * Mandelbrot set generation.
 * Split rows across threads.
 */
typedef struct {
    int *iters;
    int width, height, max_iter;
    int row_start, row_end;
    double x_min, x_max, y_min, y_max;
    long inside;
} MandelChunk;

static void mandel_worker(void *arg) {
    MandelChunk *c = (MandelChunk *)arg;
    long inside = 0;
    for (int py = c->row_start; py < c->row_end; py++) {
        for (int px = 0; px < c->width; px++) {
            double cr = c->x_min + (c->x_max - c->x_min) * px / c->width;
            double ci = c->y_min + (c->y_max - c->y_min) * py / c->height;

            double zr = 0.0, zi = 0.0;
            int iter = 0;
            while (zr * zr + zi * zi <= 4.0 && iter < c->max_iter) {
                double tmp = zr * zr - zi * zi + cr;
                zi = 2.0 * zr * zi + ci;
                zr = tmp;
                iter++;
            }

            c->iters[py * c->width + px] = iter;
            if (iter == c->max_iter)
                inside++;
        }
    }
    c->inside = inside;
}

void mandelbrot(int width, int height, int max_iter) {
    int *iters = (int *)malloc((size_t)width * height * sizeof(int));

    double x_min = -2.0, x_max = 1.0;
    double y_min = -1.5, y_max = 1.5;

    MandelChunk *chunks = (MandelChunk *)malloc(num_threads * sizeof(MandelChunk));
    int rows_per = height / num_threads;

    for (int i = 0; i < num_threads; i++) {
        chunks[i].iters = iters;
        chunks[i].width = width;
        chunks[i].height = height;
        chunks[i].max_iter = max_iter;
        chunks[i].x_min = x_min; chunks[i].x_max = x_max;
        chunks[i].y_min = y_min; chunks[i].y_max = y_max;
        chunks[i].row_start = i * rows_per;
        chunks[i].row_end = (i == num_threads - 1) ? height : (i + 1) * rows_per;
        chunks[i].inside = 0;
    }

    for (int i = 0; i < num_threads; i++)
        thpool_add_work(global_thpool, mandel_worker, &chunks[i]);
    thpool_wait(global_thpool);

    long total_inside = 0;
    for (int i = 0; i < num_threads; i++)
        total_inside += chunks[i].inside;

    printf("Mandelbrot %dx%d, max_iter=%d\n", width, height, max_iter);
    printf("Pixels in set: %ld / %ld\n", total_inside, (long)width * height);

    free(iters);
    free(chunks);
}

/*
 * Numerical integration using the midpoint rule.
 * Split intervals across threads.
 */
static double integrate_f(double x) {
    return sin(x) * log(x + 1.0) + sqrt(x);
}

typedef struct {
    long start;
    long end;
    double a, h;
    double partial_sum;
} IntegChunk;

static void integ_worker(void *arg) {
    IntegChunk *c = (IntegChunk *)arg;
    double sum = 0.0;
    for (long i = c->start; i < c->end; i++) {
        double mid = c->a + (i + 0.5) * c->h;
        sum += integrate_f(mid);
    }
    c->partial_sum = sum;
}

void numerical_integration(long num_intervals, double a, double b) {
    double h = (b - a) / num_intervals;

    IntegChunk *chunks = (IntegChunk *)malloc(num_threads * sizeof(IntegChunk));
    long per = num_intervals / num_threads;

    for (int i = 0; i < num_threads; i++) {
        chunks[i].a = a;
        chunks[i].h = h;
        chunks[i].start = i * per;
        chunks[i].end = (i == num_threads - 1) ? num_intervals : (i + 1) * per;
        chunks[i].partial_sum = 0.0;
    }

    for (int i = 0; i < num_threads; i++)
        thpool_add_work(global_thpool, integ_worker, &chunks[i]);
    thpool_wait(global_thpool);

    double sum = 0.0;
    for (int i = 0; i < num_threads; i++)
        sum += chunks[i].partial_sum;

    double result = sum * h;

    printf("Integration of f(x) over [%.2f, %.2f]\n", a, b);
    printf("Intervals:  %ld\n", num_intervals);
    printf("Result:     %.10f\n", result);

    free(chunks);
}

/*
 * Ray tracing a scene of spheres.
 * Split rows across threads.
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

typedef struct {
    unsigned char *image;
    Sphere *spheres;
    int num_spheres;
    Vec3 light, cam_origin;
    double fov;
    int width, height;
    int row_start, row_end;
    long hits;
} RTChunk;

static void rt_worker(void *arg) {
    RTChunk *c = (RTChunk *)arg;
    long hits = 0;
    for (int py = c->row_start; py < c->row_end; py++) {
        for (int px = 0; px < c->width; px++) {
            double u = (2.0 * px / c->width - 1.0) * ((double)c->width / c->height);
            double v = 1.0 - 2.0 * py / c->height;
            Vec3 dir = vec3_norm(vec3(u * c->fov, v * c->fov, 1.0));

            double closest_t = 1e20;
            int hit_idx = -1;

            for (int s = 0; s < c->num_spheres; s++) {
                double t;
                if (sphere_intersect(&c->spheres[s], c->cam_origin, dir, &t)) {
                    if (t < closest_t) {
                        closest_t = t;
                        hit_idx = s;
                    }
                }
            }

            Vec3 pixel_color;
            if (hit_idx >= 0) {
                hits++;
                Vec3 hit_point = vec3_add(c->cam_origin, vec3_scale(dir, closest_t));
                Vec3 normal = vec3_norm(vec3_sub(hit_point, c->spheres[hit_idx].center));
                Vec3 to_light = vec3_norm(vec3_sub(c->light, hit_point));
                double diffuse = vec3_dot(normal, to_light);
                if (diffuse < 0.0) diffuse = 0.0;
                double ambient = 0.1;
                double brightness = ambient + 0.9 * diffuse;
                pixel_color = vec3_scale(c->spheres[hit_idx].color, brightness);
            } else {
                double t_sky = 0.5 * (v + 1.0);
                pixel_color = vec3_add(vec3_scale(vec3(1.0, 1.0, 1.0), 1.0 - t_sky),
                                       vec3_scale(vec3(0.5, 0.7, 1.0), t_sky));
            }

            int idx = (py * c->width + px) * 3;
            c->image[idx + 0] = (unsigned char)(fmin(pixel_color.x, 1.0) * 255);
            c->image[idx + 1] = (unsigned char)(fmin(pixel_color.y, 1.0) * 255);
            c->image[idx + 2] = (unsigned char)(fmin(pixel_color.z, 1.0) * 255);
        }
    }
    c->hits = hits;
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

    RTChunk *chunks = (RTChunk *)malloc(num_threads * sizeof(RTChunk));
    int rows_per = height / num_threads;

    for (int i = 0; i < num_threads; i++) {
        chunks[i].image = image;
        chunks[i].spheres = spheres;
        chunks[i].num_spheres = num_spheres;
        chunks[i].light = light;
        chunks[i].cam_origin = cam_origin;
        chunks[i].fov = fov;
        chunks[i].width = width;
        chunks[i].height = height;
        chunks[i].row_start = i * rows_per;
        chunks[i].row_end = (i == num_threads - 1) ? height : (i + 1) * rows_per;
        chunks[i].hits = 0;
    }

    for (int i = 0; i < num_threads; i++)
        thpool_add_work(global_thpool, rt_worker, &chunks[i]);
    thpool_wait(global_thpool);

    long total_hits = 0;
    for (int i = 0; i < num_threads; i++)
        total_hits += chunks[i].hits;

    printf("Ray trace %dx%d, %d spheres\n", width, height, num_spheres);
    printf("Pixels hit: %ld / %ld\n", total_hits, (long)width * height);

    free(image);
    free(chunks);
}

/*
 * Brute-force hash cracking.
 * Split candidate space across threads, use atomic flag for early exit.
 */

static unsigned long djb2_hash(const char *str, int len) {
    unsigned long hash = 5381;
    for (int i = 0; i < len; i++) {
        hash = ((hash << 5) + hash) + (unsigned char)str[i];
    }
    return hash;
}

typedef struct {
    long start;
    long end;
    unsigned long target_hash;
    int password_len;
    const char *charset;
    int charset_size;
    atomic_int *found_flag;
    long found_at;
    char found_candidate[16];
    long checked;
} CrackChunk;

static void crack_worker(void *arg) {
    CrackChunk *c = (CrackChunk *)arg;
    char candidate[16];
    candidate[c->password_len] = '\0';
    long checked = 0;

    for (long idx = c->start; idx < c->end; idx++) {
        if (atomic_load(c->found_flag))
            break;

        long tmp = idx;
        for (int pos = c->password_len - 1; pos >= 0; pos--) {
            candidate[pos] = c->charset[tmp % c->charset_size];
            tmp /= c->charset_size;
        }

        checked++;
        if (djb2_hash(candidate, c->password_len) == c->target_hash) {
            c->found_at = idx;
            memcpy(c->found_candidate, candidate, c->password_len + 1);
            atomic_store(c->found_flag, 1);
            break;
        }
    }
    c->checked = checked;
}

void brute_force_crack(int password_len) {
    const char charset[] = "abcdefghijklmnopqrstuvwxyz";
    int charset_size = 26;

    char target[16];
    for (int i = 0; i < password_len; i++) {
        target[i] = charset[(i * 7 + 3) % charset_size];
    }
    target[password_len] = '\0';
    unsigned long target_hash = djb2_hash(target, password_len);

    printf("Hash crack: target='%s' hash=%lu len=%d\n", target, target_hash, password_len);

    long total = 1;
    for (int i = 0; i < password_len; i++) total *= charset_size;

    atomic_int found_flag = 0;
    CrackChunk *chunks = (CrackChunk *)malloc(num_threads * sizeof(CrackChunk));
    long per = total / num_threads;

    for (int i = 0; i < num_threads; i++) {
        chunks[i].start = i * per;
        chunks[i].end = (i == num_threads - 1) ? total : (i + 1) * per;
        chunks[i].target_hash = target_hash;
        chunks[i].password_len = password_len;
        chunks[i].charset = charset;
        chunks[i].charset_size = charset_size;
        chunks[i].found_flag = &found_flag;
        chunks[i].found_at = -1;
        chunks[i].checked = 0;
    }

    for (int i = 0; i < num_threads; i++)
        thpool_add_work(global_thpool, crack_worker, &chunks[i]);
    thpool_wait(global_thpool);

    long checked = 0;
    long found_at = -1;
    char found_candidate[16] = {0};
    for (int i = 0; i < num_threads; i++) {
        checked += chunks[i].checked;
        if (chunks[i].found_at >= 0) {
            found_at = chunks[i].found_at;
            memcpy(found_candidate, chunks[i].found_candidate, password_len + 1);
        }
    }

    if (found_at >= 0) {
        printf("Found:      '%s' at index %ld\n", found_candidate, found_at);
    } else {
        printf("Not found after %ld candidates\n", checked);
    }
    printf("Checked:    %ld / %ld\n", checked, total);

    free(chunks);
}

int main_worker(int argc, char *argv[]) {

    if (argc < 7) {
        printf("Usage: main_original <num_samples> <mandel_size> <max_iter> <num_intervals> <rt_size> <pw_len> <num_threads>\n");
        return 1;
    }

    // Get thread pool and num_threads
    num_threads = atoi(argv[argc-2]);
    init_global_threadpool();

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

    cleanup_global_threadpool();

    return 0;
}
