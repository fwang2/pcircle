#pragma once

#include <mpi.h>
#include <string>

#define REQUEST_TAG 1
#define REPLY_TAG 2
#define DIE_TAG 3

typedef struct {
    MPI_Request request;
    MPI_Status status;
    void* request_buf;
    void* reply_buf;
    bool pending_request;
    bool pending_reply;
    bool busy;

} CW_state_st;

typedef struct {
    char* buf;
    size_t size;
} Work_st;

class CW {
   private:
    std::string m_comm_name;
    std::string m_path;

    bool m_finalize_mpi;
    bool m_term_flag;
    int m_rank;
    int m_size;

    // worker and queue
    std::vector<CW_state_st> m_workers;
    std::queue<Work_st> m_req_queue;

    // internal
    void worker();
    void master();
    void loop();
    bool check_for_term();

   protected:
    MPI_Comm m_comm;

   public:
    CW(int, char**);
    ~CW(){};
    void begin();
    void finalize();
    void enq(Work_st work);
    void set_path(const std::string path);
    virtual void create(const std::string&) = 0;
    virtual void process_request(char*, int, char*&, int&) = 0;
    virtual void process_reply(char* buf, int size) = 0;
};
