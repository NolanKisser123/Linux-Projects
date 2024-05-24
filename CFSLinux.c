#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <semaphore.h>
#include <errno.h>
#include <unistd.h>

#define MAX_TASKS 256
#define NUM_CPUS 4

// Enum for scheduling types
enum SchedulingType {
    NORMAL,
    RR,
    FIFO
};

// Define the struct to hold the data
struct TaskData {
    int pid;
    enum SchedulingType policy;
    int static_priority;
    int dynamic_priority;
    int execution_time;
    int cpu_affinity;
    int time_slice;
    int accu_time_slice;
};

// Mutex for thread safety
pthread_mutex_t task_mutex = PTHREAD_MUTEX_INITIALIZER;

// Semaphore for signaling when tasks are added
sem_t task_added;

const struct TaskData NO_TASK = {-1, NORMAL, -1, -1, -1};

// Declare the tasks array
struct TaskData tasks[MAX_TASKS];

// Flag to signal the end of the program
int done = 0;

// Flag to signal that producer is done adding tasks to queue
int producer_finished = 0;

// Function to initialize tasks
void initialize_tasks(struct TaskData* tasks, int size) {
    for (int i = 0; i < size; ++i) {
        tasks[i].pid = -1;
        tasks[i].policy = NORMAL;
        tasks[i].static_priority = -1;
        tasks[i].execution_time = -1;
        tasks[i].cpu_affinity = -1;
    }
}

// Function to check if a task is initialized
int is_task_initialized(const struct TaskData* task) {
    return task->pid != -1 || task->static_priority != -1;
}

//Adjusts dynamic priority based on the bonus value
int adjust_priority(struct TaskData task_data, int bonus) {
    int new_dynamic_priority = task_data.static_priority - bonus + 5;
    if (new_dynamic_priority < 100){
    	return 100;
    } else if (new_dynamic_priority > 139){
        return 139;
    }
    return new_dynamic_priority;
}

// Function to add a task to the ready queue
int add_to_ready_queue(struct TaskData task_data) {
    pthread_mutex_lock(&task_mutex);
    task_data.dynamic_priority = adjust_priority(task_data, 2);
    int idx;
    for (idx = 0; idx < MAX_TASKS; ++idx) {
        if (!is_task_initialized(&tasks[idx])) {
            tasks[idx] = task_data;
            break;
        }
    }
    producer_finished = 1;
    sem_post(&task_added);
    pthread_mutex_unlock(&task_mutex);

    if (idx == MAX_TASKS) {
        errno = ENOBUFS;
        return -1;
    }

    return idx;
}

// Function to extract a task from the ready queue based on CPU affinity
struct TaskData extract_from_ready_queue(int cpu_id) {
    pthread_mutex_lock(&task_mutex);
   
    // Wait for tasks to be added if the queue is empty
    while (!done && !is_task_initialized(&tasks[0])) {
        pthread_mutex_unlock(&task_mutex);
        sem_wait(&task_added);
        pthread_mutex_lock(&task_mutex);
    }

    int idx_of_best_task = -1;
    int priority_of_best_task = 200;

    for (int idx = 0; idx < MAX_TASKS; ++idx) {
        if (!is_task_initialized(&tasks[idx]) &&
            (tasks[idx].cpu_affinity == cpu_id || tasks[idx].cpu_affinity == -1)) {
            continue;
        }

        if (priority_of_best_task > tasks[idx].static_priority) {
            idx_of_best_task = idx;
            priority_of_best_task = tasks[idx].static_priority;
        }
    }

    if (idx_of_best_task == -1) {
        pthread_mutex_unlock(&task_mutex);
        return NO_TASK;
    } else {
        struct TaskData task_to_return = tasks[idx_of_best_task];
        initialize_tasks(&tasks[idx_of_best_task], 1);
        pthread_mutex_unlock(&task_mutex);
        return task_to_return;
    }
}

// Function to simulate running a task
void run_task(struct TaskData *task_data) {
    printf("PID %d is selected for running (Remaining Time: %d)\n", task_data->pid, task_data->execution_time);
    if (task_data->static_priority < 120){
        task_data->time_slice = (140 - task_data->static_priority) * 20;
    } else{
        task_data->time_slice = (140 - task_data->static_priority) * 5;
    }
    
    if (task_data->execution_time > 0) {
        usleep(task_data->time_slice * 5000);  // sleep to simulate task running
        task_data->accu_time_slice = task_data->accu_time_slice + task_data->execution_time;
        task_data->execution_time -= task_data->time_slice*50;
        
    }
    
}

// Consumer thread function
void *cpu_thread(void *arg) {
    int cpu_id = *((int *)arg);

    while (1) {
        struct TaskData extracted_task = extract_from_ready_queue(cpu_id);

        if (extracted_task.pid != -1 && producer_finished == 1) {
            run_task(&extracted_task);

            // Check if execution time exceeds the time slice based on static priority
            if (extracted_task.execution_time > 0) {
                // Execution time remains, put task back into ready queue
                int added_idx = add_to_ready_queue(extracted_task);
                if (added_idx != -1) {
                    printf("PID %d put back into the ready queue (Remaining Time: %d), Sched Type: %d CPU Affinity: %d\n", extracted_task.pid, extracted_task.execution_time, extracted_task.policy, extracted_task.cpu_affinity);
                } else {
                    perror("Failed to put task back into the ready queue");
                }
            } else {
                // Task completed
                printf("PID %d completed.\n Sched_Type: %d\n Static Priority: %d\n Dynamic Priority: %d\n Time Slice: %d\n Accu Time Slice: %d\n CPU Affinity: %d\n Last CPU: %d\n", extracted_task.pid, extracted_task.policy, extracted_task.static_priority, extracted_task.dynamic_priority, extracted_task.time_slice, extracted_task.accu_time_slice, extracted_task.cpu_affinity, cpu_id);
            }
        } else {
            // No more tasks available for this CPU
            printf("No more tasks available for CPU %d\n", cpu_id);
            break;
        }
    }

    pthread_exit(NULL);
}

// Function to convert a string to the corresponding enum value
enum SchedulingType get_scheduling_type(const char *policy_str) {
    if (strcmp(policy_str, "NORMAL") == 0) {
        return NORMAL;
    } else if (strcmp(policy_str, "RR") == 0) {
        return RR;
    } else if (strcmp(policy_str, "FIFO") == 0) {
        return FIFO;
    } else {
        // Default to NORMAL if the string doesn't match any known type
        return NORMAL;
    }
}

// Function to parse a CSV line and store the data in the struct
int parse_csv_line(FILE *file, struct TaskData *task_data) {
    char policy_str[7]; // Temporary buffer for the policy string

    int result = fscanf(file, "%d,%6[^,],%d,%d,%d",
                        &task_data->pid,
                        policy_str,
                        &task_data->static_priority,
                        &task_data->execution_time,
                        &task_data->cpu_affinity);

    if (result == EOF) {
        // End of file
        return EOF;
    } else if (result == 0) {
        // fscanf returns 0 when it fails to match any items
        return 0;
    } else if (result != 5) {
        // Parsing error
        return -1;
    }

    // Convert the policy string to the corresponding enum value
    task_data->policy = get_scheduling_type(policy_str);

    return 1; // Success
}

int main() {
    sem_init(&task_added, 0, 0);

    FILE *file = fopen("data.csv", "r");
    if (file == NULL) {
        perror("Error opening file");
        return 1;
    }

    struct TaskData task_data;

    // Initialize tasks array
    initialize_tasks(tasks, MAX_TASKS);

    // Create CPU threads
    pthread_t cpu_threads[NUM_CPUS];
    int cpu_ids[NUM_CPUS];

    for (int i = 0; i < NUM_CPUS; ++i) {
        cpu_ids[i] = i;
        pthread_create(&cpu_threads[i], NULL, cpu_thread, (void *)&cpu_ids[i]);
    }

    while (1) {
        int result = parse_csv_line(file, &task_data);

        if (result == 1) {
            // Successfully parsed a line, add to ready queue
            int added_idx = add_to_ready_queue(task_data);
            if (added_idx != -1) {
                printf("Task %d added to the ready queue\n", task_data.pid);
            } else {
                perror("Failed to add task to the ready queue");
            }
        } else if (result == EOF) {
            // End of file
            done = 1;
            sem_post(&task_added); // Tasks have been added
            break;
        } else {
            // Parsing error
            fprintf(stderr, "Error parsing CSV line\n");
            break;
        }
    }

    fclose(file);

    // Wait for CPU threads to finish
    for (int i = 0; i < NUM_CPUS; ++i) {
        pthread_join(cpu_threads[i], NULL);
    }

    sem_destroy(&task_added);

    return 0;
}

