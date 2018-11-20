#ifndef FPROF_H
#define FPROF_H

#include <string>
#include <vector>

#include "pop/ring.h"

class Profile : public POP::Ring {
   private:
    unsigned long m_local_work_cnt = 0;
    void process_directory(std::string);

   public:
    std::string m_rootp;

    Profile(int argc, char* argv[]);
    void create() override;
    void process() override;
    void reduce_init(std::vector<unsigned long>&) override;
    void reduce_finalize(const std::vector<unsigned long>) override;
};

#endif
