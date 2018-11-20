#include <cstdio>
#include <setjmp.h>
#include <signal.h>
#include <unistd.h>

class Timeout;
static Timeout * global_timeout_instance = 0;

class Timeout {
public:
	int m_timeout;
	jmp_buf env;

	Timeout(int timeout) : m_timeout(timeout) {
		if (global_timeout_instance) {
			throw "Timeout already in use";
		}
		global_timeout_instance = this;
	}

	~Timeout() {
		stop();
		global_timeout_instance = 0;
	}

	static void alarm_handler(int signum) {
		longjmp(global_timeout_instance->env, 1);
	}

	void start() {
		Timeout * ptr = this;
		if (setjmp(env) != 0) {
			// Don't do anything except throw here, since the state
			// is... funky...
			printf("Alarm fired: %p\n", ptr);
			throw global_timeout_instance;
		}
		signal(SIGALRM, alarm_handler);
		alarm(m_timeout);
		printf("Alarm set: %p\n", ptr);
	}

	void stop() {
		alarm(0);
	}
};

#define LOOPSIZE 10000
int f() {
	int sum = 0;
	for (int i = 0; i < LOOPSIZE; i++) {
		for (int j = 0; j < LOOPSIZE; j++) {
			for (int k = 0; k < LOOPSIZE; k++) {
				sum += i*j*k;
			}
		}
	}
	printf("Done!\n");
	return sum;
}

int g() {
	try {
		Timeout timeout(1); timeout.start();
		f();
	} catch (Timeout * t) {
		printf("Timeout!\n");
	}
}

int main() {
	g();
}