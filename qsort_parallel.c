#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>
#include <math.h>
#include <unistd.h>

#define DEBUG 1
#define MIN(a, b)(((a) < (b)) ? (a) : (b));

/* Global Variables */
pthread_mutex_t lock;
sem_t SEM_WORKER;
sem_t SEM_MASTER;

/* Number of worked threads */ 
unsigned int num_threads = 4;
/* Number of els in input array */
unsigned int N = 40;

/* Total number of samples to be collected */ 
int total_samples = 16;
/* Index of current sample */ 
size_t cur_sample = 0;  

/* Array to store samples */ 
int *samples = NULL;
/* Array to store pivots */ 
int *pivots = NULL;

/* 2D Array to store all partitions */ 
int **partitions = NULL;


int cmp_func(const void * x, const void * y) {
    return ( * ( int * ) x - * ( int * ) y);
}

void * collect(void * );
void * csort(void * );
void print_arr(int * , size_t);

struct Args {
    int * arr;
    size_t start;
    size_t end;
    int * samples;
    int total_samples;
    unsigned int tid; 
};

/* initialise arrays */ 
void init_array() {
    samples = (int * ) malloc(total_samples * sizeof(int));
    pivots = (int * ) malloc((num_threads - 1) * sizeof(int));
    partitions = (int ** ) malloc( num_threads * (num_threads-1) * sizeof(int*) );
}

/* Create args for worker threads */
struct Args *create_args(unsigned int tid, int *arr) {
    struct Args * args = (struct Args * ) malloc(sizeof(struct Args));
    args -> tid = tid;
    args -> arr = arr;
    args -> start = tid * ceil((double) N / (double) num_threads);
    args -> end = MIN(N, (tid + 1) * ceil((double) N / (double) num_threads));
    args -> samples = samples;
    args -> total_samples = total_samples;
    return args;
}


int main(int argc, char ** argv) {
    int arr[] = {
        42,
        98,
        2,
        31,
        86,
        87,
        5,
        13,
        99,
        44,
        67,
        37,
        17,
        7,
        87,
        3,
        96,
        71,
        40,
        19,
        58,
        13,
        61,
        77,
        11,
        13,
        6,
        81,
        76,
        18,
        24,
        14,
        63,
        59,
        99,
        17,
        36,
        84,
        1,
        48
    };

    printf("\nOriginal Array: ");
    print_arr(arr, N);

    // initialise mutex lock 
    if (pthread_mutex_init( & lock, NULL) != 0) {
        printf("Error initialising mutex lock.");
        return 1;
    }
    // initialise semaphore 
    sem_init( &SEM_WORKER, 0, 0);
    sem_init( &SEM_MASTER, 0, 0);

    pthread_t collector_thread; // collector thread
    pthread_t workers[num_threads]; // worker threads for sorting 

    /* Initialise shared arrays */ 
    init_array();

    /* Prepare arguments for collector thread */
    struct Args * collect_args = (struct Args * ) malloc(sizeof(struct Args));
    collect_args -> samples = samples;
    collect_args -> total_samples = total_samples;

    pthread_create( & collector_thread, NULL, collect, (void * ) collect_args);


    for (int i = 0; i < num_threads; ++i) {
        /* Create arguments for ith worker thread */ 
        struct Args *args = create_args(i, arr);
        
        if (args -> start < args -> end) {
            pthread_create( & workers[i], NULL, csort, (void * ) args);
        }
    }

    /* Block until worker threads finish execution */ 
    for (int i = 0; i < num_threads; ++i) {
        pthread_join(workers[i], NULL);
    }

    pthread_mutex_destroy(&lock);

    printf("\nSorted Array: ");
    print_arr(arr, N);

}

void print_arr(int * arr, size_t num_el) {
    int * cur_index = arr;
    int i = 0;
    while (i < num_el) {
        printf("%d, ", * cur_index);
        ++cur_index;
        ++i;
    }
    printf("\n");
}

void *collect(void * arguments) {
    struct Args *args = (struct Args * ) arguments;
    int threads_posted = 0;

    sem_wait(&SEM_MASTER);
    pthread_mutex_lock(&lock); 

    printf("\n\nCollector Thread Activate....\n");
    for (int i = 0; i < args -> total_samples; ++i) {
        printf("Sample %d: %d\n", i, args -> samples[i]);
    }

    // sort samples
    qsort(args -> samples, args -> total_samples, sizeof(int), cmp_func);
    pthread_mutex_unlock( & lock); // release lock 

    // select the pivots 
    for (int i = 1; i < num_threads; ++i) {
        pivots[i - 1] = args -> samples[i * num_threads + (num_threads / 2) - 1];
    }

    for (int i = 0; i < num_threads - 1; ++i) {
        printf("Pivot %d: %d\n", i, pivots[i]);
    }

    /* Broadcast worker threads */ 
    for (int i = 0; i < num_threads; ++i) 
        sem_post(&SEM_WORKER);

    printf("Collector thread says bye!\n");
    return 0;
}

void * csort(void * args) {
    pthread_mutex_lock( &lock); // acquire lock 

    /* Get all the arguments */ 
    int * arr = ((struct Args * ) args)->arr;
    size_t start = ((struct Args * ) args)->start;
    size_t end = ((struct Args * ) args)->end;
    int * samples = ((struct Args * ) args) -> samples;
    int total_samples = ((struct Args * ) args) -> total_samples;
    int * subset = arr + start;
    unsigned int tid = ((struct Args * ) args) -> tid;

    int n = end - start;

    printf("--- CSORT ---\n");
    printf("Selected subset: ");
    print_arr(subset, end - start);
    qsort(subset, end - start, sizeof(int), cmp_func);
    printf("Sorted subset: ");
    print_arr(subset, end - start);

    int num_samples = 4;

    for (int i = 0; i < num_samples; ++i) {
        int index = i * N / (num_threads * num_threads);
        samples[cur_sample++] = subset[index];
    }

    if (cur_sample == total_samples) sem_post(&SEM_MASTER);
    pthread_mutex_unlock(&lock); // release lock    

    sem_wait(&SEM_WORKER);

    // remove from production 
    #ifdef DEBUG
    pthread_mutex_lock( & lock);
    #endif

      // these lines are executed in parallel 
    printf("Start partition!\n");

    // stores partition index for each pivot 
    int * partition = (int *) malloc((num_threads - 1) * sizeof(int));

    // partition the subarray based on pivot values 
    unsigned int cur_partition_idx = 0;
    for (int i = 0; i < end - start; ++i) {
        //printf("Subset[i] = %d, current pivot = %d\n", subset[i], pivots[cur_partition_idx]);
        if (subset[i] > pivots[cur_partition_idx]) {
            //printf("Pivot = %d, partition index = %d\n", pivots[cur_partition_idx], i - 1);
            partition[cur_partition_idx++] = i - 1;
            if (cur_partition_idx >= num_threads - 1) break;
        }
    }

    print_arr(subset, end - start);
    // print the partition
    for (int i = 0; i < num_threads - 1; ++i) {
        printf("Partition %d: index = %d\n", pivots[i], partition[i]);
    }

    // print the partitions 
    printf("Partition for %d: ", pivots[1]);
    for (int i = partition[0]+1; i <= partition[1]; ++i) {
        printf("%d,", subset[i]);
    }

    printf("\n");

    #ifdef DEBUG
    pthread_mutex_unlock( & lock);
    #endif

    return NULL;
}
