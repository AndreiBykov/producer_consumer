#include <iostream>
#include <fstream>
#include <pthread.h>
#include <semaphore.h>
#include <algorithm>
#include <csignal>
#include <sys/stat.h>
#include <sys/time.h>

class CircularBuffer {

private:
    const int SIZE;
    char **buffer;
    int indexRead;
    int indexWrite;
    pthread_mutex_t mutex;

public:

    CircularBuffer(int size) : SIZE(size) {
        buffer = new char *[size];
        indexRead = 0;
        indexWrite = 0;
        pthread_mutex_init(&mutex, NULL);
    }

    ~CircularBuffer() {
        pthread_mutex_destroy(&mutex);
        for (int i = 0; i < SIZE; ++i) {
            if (buffer[i] != nullptr)
                delete buffer[i];
        }
        delete buffer;
    }

    void add(char *str) {
        pthread_mutex_lock(&mutex);
        buffer[indexWrite] = str;
        indexWrite = (indexWrite + 1) % SIZE;
        pthread_mutex_unlock(&mutex);
    }

    char *remove() {
        pthread_mutex_lock(&mutex);
        char *result = buffer[indexRead];
        indexRead = (indexRead + 1) % SIZE;
        pthread_mutex_unlock(&mutex);
        return result;
    }
};

void fillArrayWithRandom(int *array, int length, int minInclusive, int maxExclusive);

void fillArrayFromStream(int *array, int length, std::ifstream &in);

void writeArrayToFile(int *array, int length, std::ofstream &out);

void _sem_wait(sem_t *sem);

void *producer(void *args);

void *consumer(void *args);

void *listener(void *args);

char *producerProcess(int amountOfNumbers, int *array, int i);

void consumerProcess(int amountOfNumbers, int *array, char *filename);

struct thread_info {
    int amountOfNumbers;
    CircularBuffer *buffer;
};

int num_thr;
pthread_t *threads;
pthread_barrier_t barrier;
pthread_once_t once_timer_start = PTHREAD_ONCE_INIT,
        once_timer_end = PTHREAD_ONCE_INIT;
sem_t empty_slots;
sem_t filled_slots;
bool isQuit;
sigset_t mask;

struct timeval start, end;

void timer_start(void) { gettimeofday(&start, NULL); }

void timer_end(void) { gettimeofday(&end, NULL); }

int main(int argc, char **argv) {
    if (argc < 4) {
        std::cout << "Not enough args" << std::endl;
        return 1;
    }

    int bufferSize = atoi(argv[1]);
    int amountOfNumbers = atoi(argv[2]);
    num_thr = atoi(argv[3]);
    if (num_thr < 2) {
        std::cout << "Number of threads should be great than or equals 2" << std::endl;
        return 1;
    }

    system("exec rm -r files");
    mkdir("files", 0700);

    sem_init(&empty_slots, 0, (unsigned int) bufferSize);
    sem_init(&filled_slots, 0, 0);
    isQuit = false;

    sigemptyset(&mask);
    sigaddset(&mask, SIGQUIT);
    sigaddset(&mask, SIGINT);
    sigaddset(&mask, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &mask, NULL);

    pthread_barrier_init(&barrier, NULL, (unsigned int) num_thr);

    CircularBuffer *buffer = new CircularBuffer(bufferSize);
    thread_info *info = new thread_info;
    info->buffer = buffer;
    info->amountOfNumbers = amountOfNumbers;

    pthread_t thr_listener;
    pthread_create(&thr_listener, NULL, listener, NULL);
    threads = new pthread_t[num_thr];

    for (int i = 0; i < num_thr; ++i) {
        if (i == 0)
            pthread_create(&threads[i], NULL, producer, info);
        else
            pthread_create(&threads[i], NULL, consumer, info);
    }

    for (int i = 0; i < num_thr; ++i) {
        pthread_join(threads[i], NULL);
    }

    pthread_once(&once_timer_end, timer_end);
    double elapsed_time = (end.tv_sec - start.tv_sec) + (double) (end.tv_usec - start.tv_usec) / 1000000;
    std::cout << "Result time: " << elapsed_time << "s\n";

    pthread_barrier_destroy(&barrier);

    pthread_cancel(thr_listener);
    pthread_join(thr_listener, NULL);

    std::cout << "Main finish\n";
    return 0;
}

void *producer(void *args) {
    std::cout << "Producer start\n";
    thread_info *info = (thread_info *) args;
    int array[info->amountOfNumbers];

    pthread_barrier_wait(&barrier);
    pthread_once(&once_timer_start, timer_start);
    int i;
    for (i = 1; !isQuit; ++i) {
        char *filename = producerProcess(info->amountOfNumbers, array, i);
        std::cout << "Producer i = " << i << std::endl;

        _sem_wait(&empty_slots);

        info->buffer->add(filename);
        sem_post(&filled_slots);
    }

    std::cout << "Producer finish " << i - 1 << " operations\n";
    pthread_barrier_wait(&barrier);
    pthread_once(&once_timer_end, timer_end);
    pthread_exit(NULL);
}

char *producerProcess(int amountOfNumbers, int *array, int i) {
    fillArrayWithRandom(array, amountOfNumbers, 0, 100);

    char *filename = new char[20];
    std::ofstream out;
    sprintf(filename, "files/file%d", i);
    out.open(filename);

    writeArrayToFile(array, amountOfNumbers, out);

    out.close();
    return filename;
}

void *consumer(void *args) {
    std::cout << "Consumer start\n";
    thread_info *info = (thread_info *) args;

    pthread_barrier_wait(&barrier);
    pthread_once(&once_timer_start, timer_start);

    int array[info->amountOfNumbers];
    int remains = 0;
    for (int i = 1; !isQuit || remains > 0; ++i, sem_getvalue(&filled_slots, &remains)) {

        _sem_wait(&filled_slots);
        char *filename = info->buffer->remove();
        sem_post(&empty_slots);

        std::cout << "Consumer i = " << i << std::endl;
        consumerProcess(info->amountOfNumbers, array, filename);
    }

    std::cout << "Consumer finish\n";
    pthread_barrier_wait(&barrier);
    pthread_once(&once_timer_end, timer_end);
    pthread_exit(NULL);
}

void consumerProcess(int amountOfNumbers, int *array, char *filename) {
    std::ifstream in;
    in.open(filename);
    fillArrayFromStream(array, amountOfNumbers, in);
    in.close();

    std::sort(array, array + amountOfNumbers);

    std::ofstream out;
    out.open(filename);
    writeArrayToFile(array, amountOfNumbers, out);
    out.close();

    delete filename;
}

void *listener(void *args) {
    for (;;) {
        int signo;
        int rc = sigwait(&mask, &signo);
        if (rc != 0) {
            std::cout << "error in sigwait\n";
            exit(1);
        }
        switch (signo) {
            case SIGQUIT:
                std::cout << "SIGQUIT received\n";
                isQuit = true;
                break;
            case SIGINT:
            case SIGTERM:
                std::cout << "SIGINT or SIGTERM received\n";
                for (int i = 0; i < num_thr; ++i) {
                    pthread_cancel(threads[i]);
                }
                break;

            default:
                break;
        }
    }
}

void fillArrayWithRandom(int *array, int length, int minInclusive, int maxExclusive) {
    for (int i = 0; i < length; ++i) {
        array[i] = rand() % maxExclusive + minInclusive;
    }

}

void writeArrayToFile(int *array, int length, std::ofstream &out) {
    for (int i = 0; i < length; ++i) {
        out << array[i] << " ";
    }
}

void _sem_wait(sem_t *sem) {
    while (sem_wait(sem) != 0) {}
}

void fillArrayFromStream(int *array, int length, std::ifstream &in) {
    for (int i = 0; i < length; ++i) {
        in >> array[i];
    }
}

