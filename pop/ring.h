#ifndef RING_H
#define RING_H

#include <mpi.h>
#include <queue>
#include <string>
#include <cstdlib>

#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/sinks/basic_file_sink.h"

namespace POP {

#define WORK_REQUEST_TAG 1
#define WORK_REPLY_TAG 2
#define TOKEN_TAG 3
#define TERMINATION_TAG 4
enum Tags { 
    MSG_INVALID, 
    MSG_EMPTY = 7, 
    ABORT = -2
};

enum Color { BLACK, WHITE, TERMINATE = -1 };

extern spdlog::level::level_enum  RING_LOG_LEVEL;

struct WorkItem {
    char* buf;
    size_t size;
};

/**
 * the work queue is a queue of pointers that points to a c++ string
 * alternative design would be pointing to a struct of (void * ) and (size)
 * which will represent the work item. That would be more c-like.
 *
 * However, since we are defining Ring under c++ setting, expect the client
 * program will be c++ likewise, it seems std::string will suffice, for now.
 */
class Ring {
   public:
    Ring(int argc=0, char**argv=nullptr);
    ~Ring(){};
    void set_name(const std::string& name);
    void begin();
    void finalize();
    void enqueue(WorkItem);
    WorkItem dequeue();
    unsigned int qsize();
    void enable_reduce(int interval, bool inner = true);

    // subclass must define
    virtual void create() = 0;
    virtual void process() = 0;

    // optional
    virtual void reduce_init(std::vector<unsigned long>&) {};
    virtual void reduce_finalize(const std::vector<unsigned long>){};

   private:
    MPI_Comm m_comm = MPI_COMM_NULL;
    MPI_Request m_request = MPI_REQUEST_NULL;
    std::string m_comm_name = "ring";
    int m_rank = -1;
    int m_size = -1;
    bool m_finalize_mpi = false;

    bool m_inner_reduce = false;
    bool m_reduce_enabled = false;
    double m_reduce_last = 0.0;
    double m_reduce_interval = 10.0;
    unsigned long* m_reduce_outbuf = nullptr;        // receive buffer for mpi
    std::vector<unsigned long> m_reduce_invec = {};  // store innput vector
    size_t m_reduce_size = 0;
    MPI_Request m_reduce_request = MPI_REQUEST_NULL;

    int m_token_src = -1;   // where token is from
    int m_token_dest = -1;  // where token is sent to
    Color m_token_color = BLACK;
    Color m_procs_color = WHITE;
    bool m_token_is_local = false;
    bool m_term_flag = false;
    MPI_Request m_token_send_req = MPI_REQUEST_NULL;

    bool m_pending_reduce = false;
    bool m_pending_work_request = false;
    int m_work_request_source;
    size_t m_local_work_cnt = 0;
    size_t m_total_work_cnt = 0;
    std::vector<WorkItem> m_workq = {};
    std::vector<int> m_requestors = {};

    // private logger
    std::shared_ptr<spdlog::logger> RING_LOG;

    void check_work_requests(bool cleanup);
    void worker();
    void loop();
    bool check_for_term();
    int get_next_procs();  // get next process to send work to
    void send_work(int rank, int size); // send 'size' to 'rank'
    void send_work(); // distribute work to all requestors
    void send_no_work();    // send no work to all m_requestors
    void send_no_work(int); // send no work to a rank
    void collect_work_bufs(char*&, int*&, int, int&);
    void request_work(bool cleanup);
    void recv_work(int source, int size); // receive from rank {source}
    void recv_token(); // we are getting the token
    void send_token(); 
    void token_check();
    std::string curr_state();
    void reduce_check(bool cleanup = false);
};

}  // namespace POP

#endif
