// Copyright (C) 2018, Feiyi Wang

#include <queue>
#include <string>
#include <vector>

#include <mpi.h>

#include "cw.h"
#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"

typedef struct {
    size_t dir_cnt;
    size_t file_cnt;
    size_t lnk_cnt;
    size_t total_size;
} workstat_st;

auto CW_LOG = spdlog::stdout_color_mt("cw");
auto CW_LOG_LEVEL = spdlog::level::err;

void CW::master() {
    // find free worker and sent out work request
    for (int rank = 1; rank < m_size; rank++) {
        if (!m_req_queue.empty() && !m_workers[rank].busy) {
            Work_st work = m_req_queue.front();
            MPI_Isend(work.buf, work.size, MPI_CHAR, rank, REQUEST_TAG, m_comm,
                      &m_workers[rank].request);
            m_workers[rank].request_buf =
                work.buf;  // keep the buffer pointer within Work_st
            m_workers[rank].pending_request = true;
            m_workers[rank].busy = true;
            m_req_queue.pop();
            CW_LOG->debug("rank {} -> rank {}, Isend work request", m_rank,
                           rank);
        }
    }

    // check and free work request
    for (int rank = 1; rank < m_size; rank++) {
        if (m_workers[rank].pending_request) {
            int flag = 0;
            MPI_Test(&m_workers[rank].request, &flag, MPI_STATUS_IGNORE);
            if (flag) {
                CW_LOG->debug("workers[{}].request test ok", rank);
                m_workers[rank].pending_request = false;
                free(m_workers[rank]
                         .request_buf);  // we can now free the buffer that
                                         // holds data of work request.
            }
        }
    }

    // check and process reply
    int flag = 0, size = 0;
    MPI_Status status;
    MPI_Iprobe(MPI_ANY_SOURCE, REPLY_TAG, m_comm, &flag, &status);
    if (flag) {
        MPI_Get_count(&status, MPI_BYTE, &size);
        char* buf = (char*)malloc(size);
        assert(buf != NULL);
        MPI_Recv(buf, size, MPI_BYTE, status.MPI_SOURCE, status.MPI_TAG, m_comm,
                 &status);
        process_reply(buf, size);
        m_workers[status.MPI_SOURCE].busy = false;  // free up this worker
        free(buf);
    }

    // only master can decide term condition
    if (check_for_term()) {
        CW_LOG->debug("Master decided all work done");
        m_term_flag = true;
        for (int rank = 1; rank < m_size; rank++) {
            MPI_Send(&m_term_flag, 1, MPI_CXX_BOOL, rank, DIE_TAG, m_comm);
        }
    }
}

void CW::enq(Work_st work) { m_req_queue.push(work); }

void CW::worker() {
    int size;
    MPI_Status status;
    MPI_Probe(0, MPI_ANY_TAG, m_comm, &status);

    // maybe this is not needed
    if (status.MPI_TAG == DIE_TAG) {
        m_term_flag = true;
        return;
    }

    MPI_Get_count(&status, MPI_BYTE, &size);
    char* buf = (char*)malloc(size);
    MPI_Recv(buf, size, MPI_CHAR, 0, status.MPI_TAG, m_comm, &status);
    char* reply_buf;
    int reply_size;

    if (status.MPI_TAG == REQUEST_TAG) {
        process_request(buf, size, reply_buf, reply_size);
        MPI_Send(reply_buf, reply_size, MPI_CHAR, 0, REPLY_TAG, m_comm);
        free(reply_buf);
    }

    free(buf);
}

void CW::loop() {
    while (1) {
        if (m_rank == 0) {
            master();
        } else {
            worker();
        }
        // break out of loop
        if (m_term_flag) break;
    }
}

bool CW::check_for_term() {
    bool done = true;
    if (!m_req_queue.empty()) {
        done = false;
    } else {
        for (int rank = 1; rank < m_size; rank++) {
            if (m_workers[rank].busy) {
                done = false;
                break;
            }
        }
    }
    return done;
}

CW::CW(int argc, char** argv) {
    m_term_flag = false;
    m_finalize_mpi = false;
    int mpi_initialized;

    if (MPI_Initialized(&mpi_initialized) != MPI_SUCCESS) {
        CW_LOG->debug("MPI wasn't initialized, will try do it for app.");
    }

    // now we initialize MPI

    if (!mpi_initialized) {
        if (MPI_Init(&argc, &argv) != MPI_SUCCESS) {
            CW_LOG->error("MPI initialization failed.");
            return;
        }
        m_finalize_mpi = true;
        CW_LOG->debug("MPI initialization success.");
    }

    MPI_Comm_dup(MPI_COMM_WORLD, &m_comm);
    MPI_Comm_set_name(m_comm, m_comm_name.data());
    MPI_Comm_size(m_comm, &m_size);
    MPI_Comm_rank(m_comm, &m_rank);

    // set up logging
    CW_LOG->set_level(CW_LOG_LEVEL);

    // set up workers
    if (m_rank == 0) {
        for (int i = 0; i < m_size; i++) {
            CW_state_st ps;
            ps.request_buf = NULL;
            ps.pending_request = false;
            ps.busy = false;
            m_workers.push_back(ps);
        }
    }
}

void CW::begin(void) {
    if (m_rank == 0) create(m_path);
    loop();
}

void CW::finalize(void) {
    MPI_Comm_free(&m_comm);
    if (m_finalize_mpi) {
        MPI_Finalize();
    }
}

void CW::set_path(const std::string path) { m_path = path; }
