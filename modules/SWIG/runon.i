%module runon

#define ENOSYS 89

int runon(int cpu);
int pidrunon(int cpu, int pid);
