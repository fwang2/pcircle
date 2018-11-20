// Copyright (C) 2018, Feiyi Wang
// 
// Ring algorithm:
// 
// Processes are colored WHITE or BLACK.
// They pass a TOKEN following a RING, a token can be WHITE or BLACK
// 
// Initial condition:
//      All processes are colored as WHITE
//      P0 has the token 
//      when P0 becomes idle (local termination),
//          P0 (WHITE) sends WHITE token to next process
// 
// Rules:
//      A process passes on a token when it becomes idle.
//      A BLACK process marks the token as BLACK and pass it on.
//      A WHITE process passes token in its original color (BLACK or WHITE)
//      After a process passes on a token, it becomes WHITE
// 
//
//  (1) If Pi passes task to Pj, where i > j (i.e. passing task counterclock wise)
//      Pi becomes BLACK, otherwise Pi is WHITE
// 
//  (2) If P0 receives a BLACK token, it passes on as a WHITE token
// 
//  (3) If P0 receives a WHITE token, all processes are TERMINATED
//

#include <queue>
#include <string>
#include <vector>
#include <mpi.h>
#include <stdlib.h>
#include <iostream>
#include <sstream>

#include "ring.h"


namespace POP {

spdlog::level::level_enum RING_LOG_LEVEL = spdlog::level::err;

Ring::Ring(int argc, char** argv) {
    // make sure we are in a MPI environment
    // if not, we will attempt to init it.
    int mpi_initialized = 0;

    MPI_Initialized(&mpi_initialized);

    if (!mpi_initialized) {
        std::cout << "MPI wasn't initialized, will try do it for app."
                  << "\n";

        if (MPI_Init(&argc, &argv) != MPI_SUCCESS) {
            std::cerr << "MPI initialization failed."
                      << "\n";
            return;
        }
        m_finalize_mpi = true;
        std::cout << "MPI initialization success."
                  << "\n";
    }

    MPI_Comm_dup(MPI_COMM_WORLD, &m_comm);
    MPI_Comm_set_name(m_comm, m_comm_name.data());
    MPI_Comm_size(m_comm, &m_size);
    MPI_Comm_rank(m_comm, &m_rank);
    // check environment
    if (const char* env_p = std::getenv("RING_LOG_LEVEL")) {
        if (!strcmp(env_p, "debug")) {
            RING_LOG_LEVEL = spdlog::level::debug;
        } else if (!strcmp(env_p, "info")) {
            RING_LOG_LEVEL = spdlog::level::info;
        } else if (!strcmp(env_p, "err")) {
            RING_LOG_LEVEL = spdlog::level::err;
        } else if (!strcmp(env_p, "critical")) {
            RING_LOG_LEVEL = spdlog::level::critical;
        }
    }

    if (RING_LOG_LEVEL >= spdlog::level::info) {
        RING_LOG = spdlog::stdout_color_mt("ring");
    } else {
        RING_LOG = spdlog::basic_logger_mt(
            "ring", "rank" + std::to_string(m_rank) + "-debug.txt", true);
    }

    RING_LOG->set_level(RING_LOG_LEVEL);
    // master has the token initiallly
    if (m_rank == 0) {
        m_token_is_local = true;
    }

    // identify source and destination
    m_token_src = (m_rank - 1 + m_size) % m_size;
    m_token_dest = (m_rank + 1 + m_size) % m_size;

    // set up error handler
}

void Ring::set_name(const std::string& name) {
    m_comm_name = name;
    MPI_Comm_set_name(m_comm, m_comm_name.data());
}

void Ring::enqueue(WorkItem ele) { m_workq.push_back(ele); }

WorkItem Ring::dequeue() {
    WorkItem item = m_workq.front();
    m_workq.erase(m_workq.begin());
    return item;
}

unsigned int Ring::qsize() { return m_workq.size(); }

// non-blocking send no work to one requestors
// then blocking wait
void Ring::send_no_work(int dest) {
    int nowork = 0;
    MPI_Request r;

    MPI_Isend(&nowork, 1, MPI_INT, dest, WORK_REPLY_TAG, m_comm, &r);
    MPI_Wait(&r, MPI_STATUS_IGNORE);
}

// non-blocking send no work to every requestors
// then blocking wait
void Ring::send_no_work() {
    int nowork = 0;
    std::vector<MPI_Request> request_vec;

    for (auto dest: m_requestors) {
        MPI_Request r;
        MPI_Isend(&nowork, 1, MPI_INT, dest, WORK_REPLY_TAG, m_comm, &r);
        request_vec.push_back(r);
        RING_LOG->debug("send no work reply to rank#{}", dest);
    }
    // Alternative way of getting "array" from a vector
    // MPI_Request* head = &v_requestors[0]; 
    // However, C++ .data() seems cleaner.
    MPI_Waitall(request_vec.size(), request_vec.data(), MPI_STATUSES_IGNORE);
    m_requestors.clear();
}


/**
 * snd_buf - the buffer for sent work items
 * off_buf - offset for the sent work items
 * count - # of work items to collect from workq
 */
void Ring::collect_work_bufs(char*& snd_buf, int*& off_buf, int count, int& total_size) {
    total_size = 0;
    off_buf = (int*)malloc( (count+1) * sizeof(int) ); // plus 1 is to hold total_size
    for (int i = 0; i < count; i++) {
        total_size += m_workq[i].size;
        off_buf[i+1] = m_workq[i].size;  // create offset array
    }
    
    off_buf[0] = total_size; // total size goes to first element

    snd_buf = (char*)malloc(total_size);
    char* bufp = snd_buf;
    for (int i = 0; i < count; i++) {
        memcpy(bufp, m_workq[i].buf, m_workq[i].size);
        bufp += m_workq[i].size;
        free(m_workq[i].buf); 
    }
}


void Ring::send_work() {

    
    // split the work evenly
    int worker_cnt = m_requestors.size();
    int load_cnt = m_workq.size() / (worker_cnt + 1);  // plus self

    RING_LOG->debug(
        "Before send_work, my workq size = {}, I have processed: {}, load_cnt = {}",
        m_workq.size(), m_local_work_cnt, load_cnt);

    if (load_cnt == 0) {
        send_no_work();
        return;
    }

    bool mark_self_as_black = false;
    for (auto requestor : m_requestors) {
        char* workbuf = nullptr;
        int* offsets = nullptr;
        int total_size = 0;

        // collect individual work items into the send buffer
        collect_work_bufs(workbuf, offsets, load_cnt, total_size);

        assert(workbuf != nullptr);
        assert(offsets != nullptr);

        MPI_Send(offsets, load_cnt + 1, MPI_INT, requestor, WORK_REPLY_TAG,
                 m_comm);  // plus 1 as total size
        
        MPI_Send(workbuf, total_size, MPI_CHAR, requestor, WORK_REPLY_TAG,
                 m_comm); // just the work buffer
        free(workbuf);
        free(offsets);

        // if we sent work backward, "self" becomes black
        if (m_rank != 0 && requestor < m_rank) mark_self_as_black = true;

        RING_LOG->debug("rank #{} -> rank #{}, SEND_WORK cnt={}, size={}",\
             m_rank, requestor, load_cnt, total_size);

        // now work has been sent out, we need to modify our workq to reflect.
        m_workq.erase(m_workq.begin(), m_workq.begin() + load_cnt);
    }

    m_requestors.clear();

    if (mark_self_as_black) m_procs_color = BLACK;

    // we are left with the average load for everyone (including self) +
    // whatever left in the reminder operation.
    RING_LOG->debug("After send_work, my workq size = {}", m_workq.size());
}

/**
 * probe for work request
 */
void Ring::check_work_requests(bool cleanup=false) {
    while (1) {
        MPI_Status status;
        int flag = 0;
        MPI_Iprobe(MPI_ANY_SOURCE, WORK_REQUEST_TAG, m_comm, &flag, &status);
        if (!flag) {
            break;  // no or no more work request
        }
        int buf;
        MPI_Recv(&buf, 1, MPI_INT, status.MPI_SOURCE, WORK_REQUEST_TAG,
                    m_comm, &status);
        RING_LOG->debug("check_work_request(), rank #{} is requesting",
                        status.MPI_SOURCE);
        m_requestors.push_back(status.MPI_SOURCE);

    }

    if (m_requestors.size() != 0) {
        if (m_workq.size() == 0 || cleanup) {
            send_no_work();
        } else {
            send_work();
        }
    }
}

void Ring::recv_work(int src_rank, int count) {
    // we will block recv two messages
    // first messages is offsets
    // second message is work buffer
    // once we have all the information, we will re-struct it into
    // work and put it in our workq
    MPI_Status status;
    int* offset_buf = (int*)malloc(count * sizeof(int));
    MPI_Recv(offset_buf, count, MPI_INT, src_rank, WORK_REPLY_TAG, m_comm,
             &status);

    int total_size = offset_buf[0];  // first element is total_size
    
    if (total_size == 0) {
        // total size 0 means no work reply
        free(offset_buf);
        return;
    }

    char* work_buf = (char*)malloc(total_size);
    MPI_Recv(work_buf, total_size, MPI_CHAR, src_rank, WORK_REPLY_TAG, m_comm,
             &status);

    count--;  // this is how many work items we will get


    int cur_pos = 0;

    for (int i = 0; i < count; i++) {
        WorkItem work;
        work.size = offset_buf[i + 1];  // starting from 2nd element
        char* buf = (char*)malloc(work.size);
        memcpy(buf, work_buf + cur_pos, work.size);
        work.buf = buf;
        enqueue(work);
        cur_pos += work.size;  
    }

    // we are done with both buffers
    free(offset_buf);
    free(work_buf);

    RING_LOG->debug("rank #{} <- rank #{}, RECV_WORK, enq(cnt={}), size={}", \
        m_rank, src_rank, count, total_size);

}

void Ring::request_work(bool cleanup=false) {
    // send out work request if: we don't already have one outstanding
    // if we do have outstanding request, check for reply.
    if (m_pending_work_request) {
        MPI_Status status;
        int flag;
        MPI_Iprobe(m_work_request_source, WORK_REPLY_TAG, m_comm, &flag,
                   &status);

        if (flag) {
            int count; // # of ints we will receive
            MPI_Get_count(&status, MPI_INT, &count);
            recv_work(status.MPI_SOURCE, count);
            m_pending_work_request = false;
        }
    } else if (!cleanup) {
        m_work_request_source = get_next_procs();
        if (m_work_request_source == MPI_PROC_NULL) { // no one to ask
            return;
        }
        int buf = MSG_EMPTY;
        MPI_Request request;
        MPI_Isend(&buf, 1, MPI_INT, m_work_request_source, WORK_REQUEST_TAG,
                 m_comm, &request);
        int flag = 0;
        while (!flag) {
            MPI_Test(&request, &flag, MPI_STATUS_IGNORE);
        }
        m_pending_work_request = true;
        RING_LOG->debug("rank #{} -> rank #{}, work request sent", m_rank, m_work_request_source);
    }
}

void Ring::send_token() {
    MPI_Issend(&m_token_color, 1, MPI_INT, m_token_dest, TOKEN_TAG, m_comm,
               &m_token_send_req);
    m_token_is_local = false;
}
void Ring::recv_token() {
    // we are getting the token
    // we use new token buf here since m_token_color
    // could be active from issend() to another process
    int token;
    MPI_Status status;
    MPI_Recv(&token, 1, MPI_INT, m_token_src, TOKEN_TAG, m_comm, &status);
    
    // record that token is local
    m_token_is_local = true;

    // if we have token outstanding Not sure if we can ever get here (recv a
    // token), with m_token_send_req outstanding
    if (m_token_send_req != MPI_REQUEST_NULL) {
        MPI_Wait(&m_token_send_req, &status);
    }

    // now we are 100% sure token sent is finished
    // it is safe to over write
    m_token_color = static_cast<Color>(token);

    // BLACK process receive a token, color the token as BLACK
    if (m_procs_color == BLACK) {
        m_token_color = BLACK;
    }
    // I have the token, I am WHITE now.
    // and keep token color as it was.
    m_procs_color = WHITE;  


    // P0 receive a WHITE token, this is global termination. P0 will let
    // everyone else know by sending TERMINATE token. For everyone else, when
    // receiving a TERMINATE token, will pass it on as TERMINATE token.
    if (m_rank == 0 && m_token_color == WHITE) {
        RING_LOG->debug("Master detected termination");
        m_term_flag = true;
    } else if (token == TERMINATE) {
        // we are not rank 0, we just look for TERMINATE
        m_term_flag = true;
    }

    if (m_term_flag) {
        // send the termination token
        // do not bother if I am the last rank
        m_token_color = TERMINATE;
        if (m_rank < m_size - 1) {
            send_token();
        }
        m_procs_color = TERMINATE;
    }
}

void Ring::token_check() {
    int flag;
    MPI_Status status;
    MPI_Iprobe(m_token_src, TOKEN_TAG, m_comm, &flag, &status);
    if (flag) {
        recv_token();
    }
}

// This is called when local work queue is empty, i.e. local termination
// condition is met. we now need to examine the global termination condition.
bool Ring::check_for_term() {
    // m_procs_color as TERMINATE means that I already know global termination
    // condition is met (last time when I receive a token, and I should have
    // pass on the TERMINATE token), nothing else for me to do here.
    if (m_procs_color == TERMINATE) {
        assert( m_token_color == TERMINATE);
        return true;
    }

    if (m_token_is_local) {
        // we have no work, but have the token, send it along
        if (m_rank == 0) {
            // master starts with a WHITE token
            m_token_color = WHITE;
        } else if (m_procs_color == BLACK) {
            // a BLACK process marks the token as BLACK and pass it on
            m_token_color = BLACK;
        }
        send_token();

        // now I have no work, and token is sent
        // I am WHITE
        m_procs_color = WHITE;
    } else {
        // I have no work, and I have no token
        token_check();
    }

    if (m_procs_color == TERMINATE) 
        return true;

    return false;
}

std::string Ring::curr_state() {
    std::stringstream ss;
    ss << std::ios::boolalpha;
    ss << "rank #" << m_rank << "\n";
    ss << "workq size: " << m_workq.size() << "\n";
    ss << "requestors size: " << m_requestors.size() << "\n";
    ss << "pending_work_reqeusts: " << m_pending_work_request << "\n";
    ss << "work_request_source: " << m_work_request_source << "\n";
    ss << "token is local: " << m_token_is_local << "\n";
    ss << "token color: " << m_token_color << "\n";
    ss << "process color" << m_procs_color << "\n";


    return ss.str();

}

void Ring::loop() {
    while (1) {
        // check if I have work request coming in, if yes, and my workq is not
        // empty, then split the work and send work to requestors.
        check_work_requests();

        //RING_LOG->debug("rank #{} finish check_work_reqeust()", m_rank);

        // if no work, send out work request from another rank
        if (m_workq.size() == 0) {
            request_work();
        }
        
        
        // make progress on reduce request if any
        if (m_reduce_enabled) {
            reduce_check();
        }

        // process work queue 
        // if workq empty, check for termination
        if (m_workq.size() != 0) {
            process();
            m_local_work_cnt++;
        } else {
            bool term = check_for_term();            
            if (term) break;
        }

        // RING_LOG->debug(curr_state());
    }
}

void Ring::begin() {
    RING_LOG->debug("ring begins: reduce enabled: {}, reduce_interval = {}",
                    m_reduce_enabled, m_reduce_interval);

    if (m_rank == 0) {
        create();
        RING_LOG->debug("Upon creation, my workq size = {}", m_workq.size());
    }

    loop();

    RING_LOG->debug("rank #{} is out of loop, entering cleanup", m_rank);

    // clean up all outstanding

    bool ibarrier_flag = false;
    MPI_Request request;
    int flag = 0;
    while (true) {
        if (!m_pending_work_request && m_token_send_req == MPI_REQUEST_NULL) {
            if (!ibarrier_flag) {
                MPI_Ibarrier(m_comm, &request);
                ibarrier_flag = true;
            } else {
                MPI_Test(&request, &flag, MPI_STATUS_IGNORE);
                if (flag) break;
            }
        }
        check_work_requests(true);  // set cleanup flag
        request_work(true);         // set cleanup flag
        if (m_reduce_enabled) {
            reduce_check(true);  // set cleanup flag
        }
        token_check();
        if (m_token_send_req != MPI_REQUEST_NULL) {
            // we have outstanding token
            int flag;
            MPI_Test(&m_token_send_req, &flag, MPI_STATUS_IGNORE);
        }
    }
    assert(m_pending_work_request == false);
    assert(m_token_send_req == MPI_REQUEST_NULL);
    assert(m_requestors.size() == 0);
    assert(m_reduce_request == MPI_REQUEST_NULL);

    RING_LOG->debug("rank #{} processed {} works", m_rank, m_local_work_cnt);

    MPI_Reduce(&m_local_work_cnt, &m_total_work_cnt, 1, MPI_INT, MPI_SUM, 0,
               m_comm);
    if (m_rank == 0) {
        RING_LOG->info("Total processed works: {} ", m_total_work_cnt);
    }
}

void Ring::finalize() {
    if (m_finalize_mpi) {
        MPI_Finalize();
    }
}

// this flag decide if we do inner reduce or user reduce
// we just do one of it, not both
void Ring::enable_reduce(int interval, bool inner) {
    m_reduce_enabled = true;
    m_reduce_interval = interval;
    m_inner_reduce = inner;
}

void Ring::reduce_check(bool cleanup) {

    /* if we are in cleanup mode, we just need to make sure previous reduce will
       finish, blocking wait is fine. Otherwise, we check if need to start a new
       round of reduce operation.
    */

    if (m_pending_reduce) {
        RING_LOG->debug("Have pending reduce, do MPI_Test()");
        if (cleanup) {
            MPI_Wait(&m_reduce_request, MPI_STATUS_IGNORE);
            m_pending_reduce = false;
            return;
        }

        int flag = 0;
        MPI_Status status;
        MPI_Test(&m_reduce_request, &flag, &status);
        if (!flag) return;

        if (m_rank == 0 && m_inner_reduce) {
            RING_LOG->info("work count = {}", m_total_work_cnt);
        } else if (m_rank == 0 && !m_inner_reduce) {
            // m_reduce_outbuf now has the data
            // copy them to the vec and return to caller
            std::vector<unsigned long> outvec;
            for (size_t i = 0; i < m_reduce_size; i++) {
                outvec.push_back(m_reduce_outbuf[i]);
                RING_LOG->debug("m_reduce_outbuf[{}] = {}", i,
                                m_reduce_outbuf[i]);
            }
            reduce_finalize(outvec);
            free(m_reduce_outbuf);
        }
        m_pending_reduce = false;

    } else {
        // if we don't have pending reduce and in cleanup mode
        // no need to start another around of reduce

        if (cleanup) return;

        // otherwise, we check if we need to reduce
        
        double time_now = MPI_Wtime();
        double time_next = m_reduce_last + m_reduce_interval;
        if (time_now > time_next) {
            RING_LOG -> debug("initiate new Ireduce, inner = {}", m_inner_reduce);
            // time for reduce op
            m_reduce_last = time_now;
            m_pending_reduce = true;
            if (m_inner_reduce) {
                MPI_Ireduce(&m_local_work_cnt, &m_total_work_cnt, 1,
                        MPI_UNSIGNED_LONG, MPI_SUM, 0, m_comm,
                        &m_reduce_request);
            } else {
                m_reduce_invec.clear();       // if we have any left over
                reduce_init(m_reduce_invec);  // populate with reduce input
                m_reduce_size = m_reduce_invec.size();
                RING_LOG->debug("reduce size = {}, first reduce ele = {}", m_reduce_size, m_reduce_invec[0]);
                m_reduce_outbuf = (unsigned long*)malloc(m_reduce_size *
                                                         sizeof(unsigned long));
                MPI_Ireduce(m_reduce_invec.data(), m_reduce_outbuf,
                            m_reduce_size, MPI_UNSIGNED_LONG, MPI_SUM, 0,
                            m_comm, &m_reduce_request);
            }
        }
    }
}


int Ring::get_next_procs() {
    if (m_size > 1) {
        unsigned int seed = (unsigned)m_size;
        return rand_r(&seed) % m_size;
    } else {
        // just root, no one to ask
        return MPI_PROC_NULL;
    }
}

}  // namespace POP