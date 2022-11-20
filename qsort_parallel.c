// COMP3230 Programming Assignment Two
// The sequential version of the sorting using qsort

/*
# Filename: psort.c
# Student name and No.: Yashwardhann Kumar 3035662699
# Development platform: Workbench2
# Remark: run this file using the command "gcc psort.c -lpthread -lm"
*/

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/time.h>
#include <pthread.h>
#include <semaphore.h>
#include <math.h>
#include <unistd.h>



int checking(unsigned int *, long);
int compare(const void *, const void *);

// global variables
long size;  // size of the array
unsigned int nthreads; // number of threads
unsigned int * intarr; // array of random integers


#define MIN(a, b)(((a) < (b)) ? (a) : (b));
#define MAX(a, b)(((a) < (b)) ? (b) : (a));

/* Global Variables */
pthread_mutex_t lock;
sem_t SEM_WORKER;
sem_t SEM_MASTER;
pthread_barrier_t SORT_BARRIER;
pthread_barrier_t COLLECT_BARRIER;

/* Number of worked threads */ 
unsigned int num_threads = -1;

/* Number of els in input array */
unsigned int N = -1;

/* Total number of samples to be collected */ 
int total_samples = 0;
/* Index of current sample */ 
size_t cur_sample = 0;  

/* Array to store samples */ 
int *samples = NULL;
/* Array to store pivots */ 
int *pivots = NULL;

/* 2D Array to store all partitions */ 
int **partitions = NULL;


int cmp_func(const void * x, const void * y) {
    return ( *(int *) x - * (int *) y);
}

void *collect(void *);
void *csort(void *);
void print_arr(int * , size_t);

struct Args {
    int *arr;
    size_t start;
    size_t end;
    int *samples;
    int total_samples;
    unsigned int tid; 
};

/* initialise shared arrays */ 
void init_array() {
    samples = (int *) malloc(total_samples * sizeof(int));
    pivots = (int *) malloc((num_threads - 1) * sizeof(int));
    partitions = (int **) malloc( num_threads * sizeof(int*) );

    // initialise partitions 
    for (int i = 0; i < num_threads; ++i) {
        /* last element denotes current index */ 
        partitions[i] = (int *) malloc( (N+1) * sizeof(int) );
    }
}

void print_partitions() {
    for (int i = 0; i < num_threads; ++i) {
        printf("Partition %d\n", i);
        for (int j = 0; j < partitions[i][N]; ++j) {
            printf("%d,", partitions[i][j]);
        }
        printf("\n");
    }
}

void merge_partitions(int *arr) {
    size_t idx = 0;
    for (int i = 0; i < num_threads; ++i) {
        for (int j = 0; j < partitions[i][N]; ++j) {
            arr[idx++] = partitions[i][j];
        }
    }
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


int sort(int *arr, size_t sz, unsigned int nthreads) {
    num_threads = (nthreads != -1) ? nthreads : 2;

    N = sz;
    total_samples = num_threads * num_threads;

    #ifdef DEBUG

    printf("\nOriginal Array: ");
    print_arr(arr, N);

    #endif

    // initialise mutex lock 
    if (pthread_mutex_init( & lock, NULL) != 0) {
        printf("Error initialising mutex lock.");
        return 1;
    }
    // initialise semaphore 
    sem_init(&SEM_WORKER, 0, 0);
    sem_init(&SEM_MASTER, 0, 0);
    pthread_barrier_init(&SORT_BARRIER, NULL, num_threads);
    pthread_barrier_init(&COLLECT_BARRIER, NULL, num_threads);

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
            pthread_create( &workers[i], NULL, csort, (void * ) args);
        }
    }

    /* Block until worker threads finish execution */ 
    for (int i = 0; i < num_threads; ++i) {
        pthread_join(workers[i], NULL);
       
    }

    pthread_mutex_destroy(&lock);

    #ifdef DEBUG

    printf("\n\n\n====== MAIN THREAD ======\n");
    print_partitions();
    
    #endif

    // Merge all partitions 
    merge_partitions(arr);

    #ifdef DEBUG
    print_arr(arr, N);
    #endif
}

void print_arr(int * arr, size_t num_el) {
    int * cur_index = arr;
    int i = 0;
    while (i < num_el) {
        printf("%d, ", *cur_index);
        ++cur_index;
        ++i;
    }
    printf("\n");
}

void *collect(void * arguments) {
    struct Args *args = (struct Args *) arguments;
    int threads_posted = 0;

    sem_wait(&SEM_MASTER);
    pthread_mutex_lock(&lock); 

    #ifdef DEBUG
    print_arr(samples, total_samples);
    #endif
    // sort samples
    qsort(samples, total_samples, sizeof(int), cmp_func);
    pthread_mutex_unlock(&lock); // release lock 

    // select the pivots 
    for (int i = 1; i < num_threads; ++i) {
        unsigned int index = i * num_threads + (num_threads / 2) - 1;
        #ifdef DEBUG
        printf("index: %d\n", index);
        printf("Chosen sample: %d\n", samples[index]);
        #endif
        pivots[i-1] = samples[index];
    }
    
    #ifdef DEBUG
    print_arr(pivots, num_threads-1);
    #endif

    /* Broadcast worker threads */ 
    for (int i = 0; i < num_threads; ++i) 
        sem_post(&SEM_WORKER);

    return 0;
}

void *csort(void * args) {
    /* Get all the arguments */ 
    int *arr = ((struct Args * ) args)->arr;
    size_t start = ((struct Args * ) args)->start;
    size_t end = ((struct Args * ) args)->end;
    int *samples = ((struct Args * ) args) -> samples;
    int total_samples = ((struct Args * ) args) -> total_samples;
    int *subset = arr + start;
    unsigned int tid = ((struct Args * ) args) -> tid;

    int n = end - start;

    #ifdef DEBUG
    printf("== THREAD %d ==\n", tid);
    printf("Chosen Subset - ");
    print_arr(subset, end-start);
    #endif
   
    qsort(subset, end - start, sizeof(int), cmp_func);
   
    #ifdef DEBUG
    printf("Sorted Subset - ");
    print_arr(subset, end-start);
    #endif
    
    int num_samples = num_threads;
    
    pthread_mutex_lock( &lock); 
    for (int i = 0; i < num_samples; ++i) {
        int index = i * N / (num_threads * num_threads);
        samples[cur_sample++] = subset[index];
    }

    #ifdef DEBUG
    printf("Thread %d finished posting...\n", tid);
    #endif

    pthread_mutex_unlock(&lock); // release lock    

    pthread_barrier_wait(&COLLECT_BARRIER);
    sem_post(&SEM_MASTER);
    sem_wait(&SEM_WORKER);

    // remove from production 
   
    //stores partition index for each pivot 
    int * partition = (int *) malloc(num_threads * sizeof(int));
    for (int i = 0; i < num_threads; ++i) {
        partition[i] = -1;
    }
    // partition the subarray based on pivot values 
    unsigned int cur_partition_idx = 0;
    for (int i = 0; i < end - start; ++i) {
        while (i < end-start && subset[i] > pivots[cur_partition_idx]) {
            partition[cur_partition_idx++] = i-1;
            if (cur_partition_idx >= num_threads - 1) break;
        }
        if (cur_partition_idx >= num_threads - 1) break;
    }

    #ifdef DEBUG
    printf("\n\n == THREAD %d\n", tid);
    printf("Subset: ");
    print_arr(subset, end-start); 
    for (int i = 0; i < num_threads-1; ++i) {
        printf("Pivot: %d, ", pivots[i]); printf("Index: %d\n", partition[i]);
    }
    #endif


    pthread_mutex_lock( & lock);
    unsigned int last_pindex = -1;
    for (int i = 0; i < num_threads-1; ++i) {
        if (partition[i] == -1) continue;
        last_pindex = i;
        size_t pstart = (i > 0) ? partition[i-1] + 1 : 0; 
        for (int j = pstart; j <= partition[i]; ++j) { 
            
            #ifdef DEBUG
            printf("%d goes to %d\n", subset[j], i);
            #endif

            partitions[i][partitions[i][N]++] = subset[j];
        }
    }
    // Add the last partition 
    if (partition[last_pindex] > -1) {
        for (int i = partition[last_pindex]+1; i < end-start; ++i) {
            #ifdef DEBUG
            printf("%d goes to %d\n", subset[i], num_threads-1);
            #endif
            partitions[last_pindex+1][partitions[last_pindex+1][N]++] = subset[i];
        }

    }
    pthread_mutex_unlock( & lock);
   
    pthread_barrier_wait(&SORT_BARRIER);
 
    qsort(partitions[tid], partitions[tid][N], sizeof(int), cmp_func);

    #ifdef DEBUG
    printf("\n--- PARTITIONS ---\n");
    printf("Partition %d: ", tid);

    print_arr(partitions[tid], partitions[tid][N]);
    printf("Size = %d\n", partitions[tid][N]);
    #endif

    // printf("Exiting...");

    return NULL;
}


/* ########### */ 
/* RUNNER CODE */ 
/* ########### */ 
int main (int argc, char **argv)
{
	long i, j;
	struct timeval start, end;

	if ((argc < 2))
	{
		printf("Usage: seq_sort <number> <nthreads?>\n");
		exit(0);
	}
	  
	size = atol(argv[1]);
	nthreads = (argc > 2) ? atol(argv[2]) : 4;

	intarr = (unsigned int *)malloc(size*sizeof(unsigned int));
	if (intarr == NULL) {
        perror("malloc"); exit(0); 
    }
	
	// set the random seed for generating a fixed random
	// sequence across different runs
    char * env = getenv("RANNUM");  //get the env variable
    if (!env)                       //if not exists
        srandom(3230);
    else
        srandom(atol(env));
	
	for (i=0; i<size; i++) {
		intarr[i] = random();
	}
	
	// measure the start time
	gettimeofday(&start, NULL);
	
	// just call the qsort library
	// replace qsort by your parallel sorting algorithm using pthread
	
	if (nthreads > 1) {
		sort(intarr, size, nthreads);	
	} else {
		qsort(intarr, size, sizeof(int), cmp_func);
	}
	
	// measure the end time
	gettimeofday(&end, NULL);
	
	if (!checking(intarr, size)) {
		printf("The array is not in sorted order!!\n");
	}
	
	printf("Total elapsed time: %.4f s\n", (end.tv_sec - start.tv_sec)*1.0 + (end.tv_usec - start.tv_usec)/1000000.0);
	  
	free(intarr);
	return 0;
}

int compare(const void * a, const void * b) {
	return (*(unsigned int *)a>*(unsigned int *)b) ? 1 : ((*(unsigned int *)a==*(unsigned int *)b) ? 0 : -1);
}

int checking(unsigned int * list, long size) {
	long i;
	printf("First : %d\n", list[0]);
	printf("At 25%%: %d\n", list[size/4]);
	printf("At 50%%: %d\n", list[size/2]);
	printf("At 75%%: %d\n", list[3*size/4]);
	printf("Last  : %d\n", list[size-1]);
	for (i=0; i<size-1; i++) {
		if (list[i] > list[i+1]) {
		  return 0;
		}
	}
	return 1;
}
