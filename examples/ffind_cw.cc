#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <mpi.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <iostream>
#include <list>
#include <locale>
#include <queue>
#include <regex>  // regular expression
#include <string>

#include "CLI11.hpp"
#include "ffind_cw.pb.h"
#include "cw.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"
#include "utils.h"

using namespace utils;

struct Args {
    std::string path;
    std::string sizeinfo;
    std::string name;
    std::string group;
    std::string user;
    std::string time;
    std::string stype;

    int type;
    int dirsize;

    bool debug_flag;
    bool info_flag;
    bool warn_flag;
    bool size_greater;
    bool size_less;
    bool size_equal;
    bool do_delete;
    bool do_print;
    bool do_stat;
    bool do_profile;

    size_t size_thres;
} args;

struct FStat {
    uint64_t dir_cnt;
    uint64_t file_cnt;
    uint64_t lnk_cnt;
    uint64_t skip_cnt;
    uint64_t total_size;
    uint64_t avg_file_size;
    uint64_t avg_dir_size;
    uint64_t max_dir;
    uint64_t max_file;
    uint64_t zero_file_cnt;

    double elapsed;

} fst = {};

struct WorkerStat {
    uint64_t item_cnt;
} work_stat;

extern spdlog::level::level_enum CW_LOG_LEVEL;
auto FIND_LOG = spdlog::stdout_color_mt("ffind");
CLI::App app{"Parallel Find"};

/**
 * Match: type, name, size, time, in that order
 */

__inline__ static bool match_name(std::string name) { return true; }

__inline__ static bool match_type(int type) { return (type == args.type); }

__inline__ static bool match_size(size_t fsize) {
    if ((args.size_greater && fsize > args.size_thres) ||
        (args.size_less && fsize < args.size_thres) ||
        (args.size_equal && fsize == args.size_thres)) {
        return true;
    }

    return false;
}

__inline__ static bool match_date() { return true; }

__inline__ static void do_something(std::string fn, off_t size) {
    if (args.do_delete) {
        int ret = unlink(fn.c_str());
        if (!ret) {
            FIND_LOG->warn("Error unlink: {}, {}", fn, strerror(errno));
        }
        if (args.do_print) {
            std::cout << "unlink -> " << fn << "  (" << format(size) << ")"
                      << std::endl;
        }
    } else {
        if (args.do_stat)
            std::cout << fn << "  (" << (format(size)) << ")" << std::endl;
        else
            std::cout << fn << std::endl;
    }
}

// explicitly call base class constructor
class Find : public CW {
   private:
    void process_file_reply(WorkReply &);
    void process_dir_reply(WorkReply &);
    void process_dentry(WorkRequest &, int type);
    bool new_dir_entries(std::string dp, char *&buf, int &size);
    bool new_fileinfo(std::string fn, char *&buf, int &size);
    off_t stat_file(std::string fn, FileInfo *fi_ptr);
    void add_to_workq(WorkRequest &);

   public:
    Find(int argc, char *argv[]) : CW(argc, argv){};
    void create(const std::string &);
    void process_request(char *, int, char *&, int &);
    void process_reply(char *, int);
};

void Find::add_to_workq(WorkRequest &req) {
    Work_st work;
    work.size = req.ByteSizeLong();
    work.buf = (char *)malloc(work.size);
    req.SerializePartialToArray(work.buf, work.size);
    enq(work);
}

void Find::process_dentry(WorkRequest &req, int d_type) {
    bool valid_entry = true;

    if (d_type == DT_DIR) {
        req.set_type(MsgType::REQ_DIR_SCAN);
        fst.dir_cnt++;
    } else if (d_type == DT_REG) {
        req.set_type(MsgType::REQ_FILE_INFO);
        fst.file_cnt++;
    } else if (d_type == DT_LNK) {
        fst.lnk_cnt++;
        valid_entry = false;
    } else {
        fst.skip_cnt++;
        valid_entry = false;
        FIND_LOG->warn("Ignoring: {} -> {}", req.name(), dtype_str(d_type));
    }

    if (valid_entry) {
        if (args.do_stat)
            add_to_workq(req);
        else if (d_type == DT_DIR)
            add_to_workq(req);
        else if (d_type == DT_REG && match_type(d_type) &&
                 match_name(req.name()))
            do_something(req.name(), 0);
    }
}

void Find::create(const std::string &pth) {
    DIR *dir;
    struct dirent *ent;

    if ((dir = opendir(pth.data())) != NULL) {
        while ((ent = readdir(dir)) != NULL) {
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
                continue;
            auto req = WorkRequest();
            std::string fullname = pth + "/" + std::string(ent->d_name);
            req.set_name(fullname.data());
            process_dentry(req, ent->d_type);
        }
        closedir(dir);
    } else {
        FIND_LOG->error("opendir(): {} -> {}", pth, strerror(errno));
    }
    FIND_LOG->debug("create(): finish scanning: {}", pth);
}

off_t Find::stat_file(std::string fn, FileInfo *fi_ptr) {
    // given a file name, stat and fill the file info
    struct stat sb;
    if (lstat(fn.c_str(), &sb) == -1) {
        fi_ptr->set_status(false);
        FIND_LOG->warn("stat: {} failed, {}", fn, strerror(errno));
        fst.skip_cnt++;
        return -1;
    } else {
        fi_ptr->set_st_mode(sb.st_mode);
        fi_ptr->set_st_size(sb.st_size);
        fi_ptr->set_status(true);
    }
    return sb.st_size;
}

void Find::process_file_reply(WorkReply &reply) {
    int count = reply.finfo_size();
    for (int i = 0; i < count; i++) {
        FileInfo fi = reply.finfo(i);
        fst.total_size += fi.st_size();
        if (fi.st_size() > fst.max_file) fst.max_file = fi.st_size();
    }
}

void Find::process_dir_reply(WorkReply &reply) {
    int count = reply.dentries_size();
    if (count > fst.max_dir) fst.max_dir = count;
    for (int i = 0; i < count; i++) {
        auto dentry = reply.dentries(i);
        auto req = WorkRequest();
        req.set_name(dentry.name());
        process_dentry(req, dentry.type());
    }
}

/**
 * create WorkReply, containing FileInfo
 * serialized into "buf"
 */

bool Find::new_fileinfo(std::string fn, char *&buf, int &size) {
    WorkReply reply = WorkReply();
    reply.set_type(MsgType::REPLY_FILE_INFO);
    auto fi_ptr = reply.add_finfo();
    fi_ptr->set_name(fn);

    // size should have been more generic condition
    if (args.do_stat) {
        int dtype = -1;
        off_t fsize = 0;
        fsize = stat_file(fn, fi_ptr);
        mode_t mode = fi_ptr->st_mode();

        if (S_ISREG(mode))
            dtype = DT_REG;
        else if (S_ISDIR(mode))
            dtype = DT_DIR;
        else if (S_ISLNK(mode))
            dtype = DT_LNK;
        // FIND_LOG->warn("mode: {}, type: {}, name:{}: size:{}, date:{}", mode,
        //                match_type(dtype), match_name(fn), match_size(fsize),
        //                match_date());
        if (match_type(dtype) && match_name(fn) && match_size(fsize) &&
            match_date()) {
            do_something(fn, fsize);
        }
    }

    size = reply.ByteSizeLong();
    buf = (char *)malloc(size);
    reply.SerializePartialToArray(buf, size);
    return true;  // ignore failure case.
}

/* scan a path, and prep buf for transfer */
bool Find::new_dir_entries(std::string dp, char *&buf, int &size) {
    bool retval = true;
    DIR *dir;
    struct dirent *ent;

    // doesn't matter if we do stat, do_something.
    if (match_type(DT_DIR) && match_name(dp)) {
        do_something(dp, 0);
    }

    WorkReply reply = WorkReply();
    reply.set_type(MsgType::REPLY_DIR_SCAN);
    if ((dir = opendir(dp.data())) != NULL) {
        while ((ent = readdir(dir)) != NULL) {
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
                continue;
            auto dentry = reply.add_dentries();
            std::string fullp = dp + "/" + std::string(ent->d_name);
            dentry->set_name(fullp);
            dentry->set_type(ent->d_type);
            FIND_LOG->debug("add dentry: {}", fullp);
        }
        closedir(dir);
        size = reply.ByteSizeLong();
        buf = (char *)malloc(size);
        reply.SerializePartialToArray(buf, size);
    } else {
        FIND_LOG->warn("opendir(): [{}] -> {}", dp, strerror(errno));
        retval = false;
    }
    return retval;  // ignore failure case.
}

void Find::process_request(char *in_buf, int in_size, char *&out_buf,
                           int &out_size) {
    bool retval = false;
    WorkRequest req;
    req.ParseFromArray(in_buf, in_size);

    if (req.type() == MsgType::REQ_DIR_SCAN) {
        retval = new_dir_entries(req.name(), out_buf, out_size);
    } else if (req.type() == MsgType::REQ_FILE_INFO)
        retval = new_fileinfo(req.name(), out_buf, out_size);

    if (!retval) {
        WorkReply reply = WorkReply();
        reply.set_type(MsgType::ERROR);
        reply.set_errmsg("Error processing: " + req.name());
        out_size = reply.ByteSizeLong();
        out_buf = (char *)malloc(out_size);
        reply.SerializePartialToArray(out_buf, out_size);
        FIND_LOG->warn("reponse error, type={}, name={}", req.type(),
                       req.name());
    }
}

void Find::process_reply(char *buf, int size) {
    WorkReply reply;
    reply.ParseFromArray(buf, size);
    if (reply.type() == MsgType::REPLY_FILE_INFO)
        process_file_reply(reply);
    else if (reply.type() == MsgType::REPLY_DIR_SCAN)
        process_dir_reply(reply);
    else
        FIND_LOG->warn("Unknown work reply, type = {}", reply.type());
}

std::string validate_size(const std::string &size) {
    std::regex r_size(R"((>|<|\+|\-|=)([[:digit:]]+)(c|C|k|K|m|M|g|G|t|T))");  // >25M
    std::smatch match;
    if (regex_search(size, match, r_size)) {
        if (!match[1].compare("+") || !match[1].compare(">")) {
            args.size_greater = true;
        } else if (!match[1].compare("-") || !match[1].compare("<")) {
            args.size_less = true;
        } else {
            args.size_equal = true;
        }
        char unit = toupper(match[3].str().at(0));
        args.size_thres = conv_size(std::stoi(match[2].str()), unit);
    } else {
        return "Can not parse size info: " + size;
    }
    return "";
}

/**
 * check if we need to do stat based on the command line options.
 */
bool check_do_stat() {
    if (app.count("--size") > 0) return true;

    if (app.count("--time") > 0) return true;

    if (args.do_profile) return true;

    return false;
}

bool parse_args(int myrank, int argc, char *argv[]) {
    // retval is a flag that indicates if there is any parsing error
    // there can be only one process doing the call of app.exit(e)
    // in this case, master does that. All other processes, in the event
    // of parse error, will set retval to a non -1.
    // All processes agree to if there is a parsing error base on if the
    // retval is -1.

    int retval = -1;
    args.do_delete = false;
    args.do_print = false;

    app.set_help_all_flag("--help-all", "Expand all help");

    app.add_option("path", args.path, "Destination path")
        ->required()
        ->check(CLI::ExistingDirectory);

    app.add_flag("--delete", args.do_delete, "delete?");
    app.add_flag("--print", args.do_print, "print?");
    app.add_option("--size", args.sizeinfo, "by size ")->check(validate_size);
    app.add_flag("--profile", args.do_profile, "profiling?");
    app.add_option("--dirsize", args.dirsize, "by dir size");

    app.add_option("--name", args.name, "by name pattern");
    app.add_option("--time", args.time, "by time");  // newer or older
    app.add_option("--group", args.group, "by group");
    app.add_option("--user", args.user, "by user");
    app.add_set("--type", args.stype, {"link", "file", "dir"}, "by type");
    app.add_flag("--debug", args.debug_flag, "enable debug message");
    app.add_flag("--info", args.info_flag, "enable logging at info");
    app.add_flag("--warn", args.warn_flag, "enable logging at warn ");

    try {
        app.parse(argc, argv);
    } catch (const CLI::ParseError &e) {
        if (myrank == 0)
            retval = app.exit(e);
        else
            retval = 1;
    }
    return true ? retval == -1 : false;
}

void output_summary() {
    long long total = fst.dir_cnt + fst.file_cnt + fst.lnk_cnt + fst.skip_cnt;
    std::cout << "\n\nScanned a total of " << format(total) << " entries, "
              << "elapsed time: " << format_time(fst.elapsed)
              << ", rate: " << (int)(total / fst.elapsed) << "/s \n\n";
    return;
}

void epilog() {
    using namespace std;

    if (!args.do_profile) {
        output_summary();
        return;
    }

    cout << "\n\n";
    cout << setw(30) << left << "Number of files:" << setw(15) << left
         << format(fst.file_cnt) << "\n";
    cout << setw(30) << left << "Number of directories:" << setw(15) << left
         << format(fst.dir_cnt) << "\n";
    cout << setw(30) << left << "Number of links:" << setw(15) << left
         << format(fst.lnk_cnt) << "\n";
    cout << setw(30) << left << "Total file size:" << setw(15) << left
         << format_bytes(fst.total_size) << "\n";
    cout << setw(30) << left << "Average file size:" << setw(15) << left
         << format_bytes(fst.total_size / fst.file_cnt) << "\n";
    cout << setw(30) << left << "Average directory entries:" << setw(15) << left
         << format(fst.file_cnt / fst.dir_cnt) << "\n";
    cout << setw(30) << left << "Max file size:" << setw(15) << left
         << format_bytes(fst.max_file) << "\n";
    cout << setw(30) << left << "Max directory entries:" << setw(15) << left
         << format(fst.max_dir) << "\n";

    output_summary();
}

int main(int argc, char **argv) {
    int rank, size;
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    bool flag = parse_args(rank, argc, argv);

    if (flag) {
        if (size == 1) {
            std::cerr << "\n We need at least 2 procs run.\n" << std::endl;
            exit_app(EXIT_SUCCESS);
        }

        args.do_stat = check_do_stat();
        if (args.debug_flag)
            FIND_LOG->set_level(spdlog::level::debug);
        else if (args.info_flag)
            FIND_LOG->set_level(spdlog::level::info);
        else if (args.warn_flag)
            FIND_LOG->set_level(spdlog::level::warn);
        else
            FIND_LOG->set_level(spdlog::level::err);

        args.path = abs_path(args.path);

        if (args.path.empty()) exit_app(EXIT_SUCCESS);

        if (app.count("--type") > 0) {
            if (args.stype.compare("file") == 0) {
                args.type = DT_REG;
            } else if (args.stype.compare("dir") == 0) {
                args.type = DT_DIR;
            } else if (args.stype.compare("link") == 0) {
                args.type = DT_LNK;
            } else {
                throw;
            }
        }

        double start = MPI_Wtime();
        Find find(argc, argv);
        find.set_path(args.path);
        find.begin();
        find.finalize();
        double end = MPI_Wtime();
        fst.elapsed = end - start;
        if (rank == 0) {
            epilog();
        }
    }

    exit_app(EXIT_SUCCESS);
}
