#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <mpi.h>

#include "pop/ring.h"
#include "flist.h"
using namespace POP;

void Flist::reduce_init(std::vector<unsigned long>& invec) {
    invec.push_back(local_work_cnt_);
}

void Flist::reduce_finalize(const std::vector<unsigned long> outvec) {
    if (rank_ == 0) {
        FLIST_LOG->info("processed item count: {}", outvec[0]);
    }
}

void Flist::process_directory(std::string pth) {
    DIR* dir;
    struct dirent* ent;

    if ((dir = opendir(pth.data())) != NULL) {
        int dircnt = 0;
        while ((ent = readdir(dir)) != NULL) {
            WorkItem wi;
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
                continue;
            std::string fullname = pth + "/" + std::string(ent->d_name);
            wi.size = fullname.size() + 1;  // plus 1 for null terminator
            wi.buf = (char*)malloc(wi.size);
            strcpy(wi.buf, fullname.c_str());
            enqueue(wi);
            dircnt++;
        }
        closedir(dir);
    } else {
        FLIST_LOG->warn("opendir(): {} -> {}", pth, strerror(errno));
    }
}

void Flist::create() {
    process_directory(rootp_);
}


void Flist::process() {
    local_work_cnt_ ++;
    WorkItem wi = dequeue();
    auto pth = std::string(wi.buf);
    FLIST_LOG->debug("processing #{}", pth);
    free(wi.buf);

    struct stat sb;
    if (lstat(pth.c_str(), &sb) == -1) {
        FLIST_LOG->warn("stat: {} failed, {}", pth, strerror(errno));
        return;
    }
    fitem_t ele;
    ele.have_stat = true;
    ele.fi_name = pth;
    ele.fi_mode = sb.st_mode;
    ele.fi_ino = sb.st_ino;
    ele.fi_size = sb.st_size;
    ele.fi_atime = sb.st_atime;
    ele.fi_mtime = sb.st_mtime;
    ele.fi_ctime = sb.st_ctime;
    ele.fi_blksize = sb.st_blksize;
    ele.fi_blocks = sb.st_blocks;
    ele.fi_nlink = sb.st_nlink;
    ele.fi_uid = sb.st_uid;
    ele.fi_gid = sb.st_gid;

    switch (sb.st_mode & S_IFMT) {
        case S_IFBLK:
            break;
        case S_IFCHR:
            break;
        case S_IFDIR:
            process_directory(pth);
            break;
        case S_IFIFO:
            break;
        case S_IFLNK:
            break;
        case S_IFREG:
            break;
        case S_IFSOCK:
            break;
        default:
            break;
    }
    flist_.push_back(ele);
}

std::list<fitem_t>& Flist::treewalk(bool save_stat) {
    begin();
    finalize();
    return flist_;
}