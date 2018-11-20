#ifndef FLIST_H
#define FLIST_H
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#if HAVE_BOOST
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/string.hpp>
#endif

#include <list>
#include <string>
#include "pop/ring.h"


struct fitem_t {
   public:
    bool have_stat;
    std::string fi_name;
    mode_t fi_mode;
    ino_t fi_ino;
    nlink_t fi_nlink;
    uid_t fi_uid;
    gid_t fi_gid;
    off_t fi_size;
    time_t fi_atime;
    time_t fi_mtime;
    time_t fi_ctime;
    blksize_t fi_blksize;
    blkcnt_t fi_blocks;
#ifdef HAVE_BOOST
    //  private:
    //  friend class boost::serialization::access;
    template <typename Archive>
    void serialize(Archive &ar, const unsigned int version) {
        ar &fi_name;
        ar &fi_mode;
        ar &have_stat;
        ar &fi_ino;
        ar &fi_nlink;
        ar &fi_uid;
        ar &fi_gid;
        ar &fi_size;
        ar &fi_atime;
        ar &fi_mtime;
        ar &fi_ctime;
        ar &fi_blksize;
        ar &fi_blocks;
    }
#endif
};

class Flist : POP::Ring {
   public:
    Flist(std::string path, int rank, int size)
        : Ring(), rootp_(path), rank_{rank}, size_{size} {};
    void create() override;
    void process() override;
    void reduce_init(std::vector<unsigned long> &) override;
    void reduce_finalize(const std::vector<unsigned long>) override;
    std::list<fitem_t>& treewalk(bool save_stat=true);

   private:
    std::string rootp_;
    unsigned long local_work_cnt_ = 0;
    int rank_ = 0;
    int size_ = 0;
    std::shared_ptr<spdlog::logger> FLIST_LOG = spdlog::stdout_color_mt("flist");
    void process_directory(std::string);
    // main data repo
    std::list<fitem_t> flist_ = {};
};

#endif


