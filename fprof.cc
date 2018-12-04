#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <unordered_map>
#include <sstream>   // std::istringstream
#include <mpi.h>
#include <algorithm> // std::sort

#include "cli11.h"
#include "fprof.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"
#include "utils.h"
#include "flist.h"

using namespace utils;
using namespace POP;

// global
auto FP_LOG = spdlog::stdout_color_mt("fprof");
CLI::App app{"Parallel Profiling"};
int size, rank;
spdlog::level::level_enum FP_LOG_LEVEL = spdlog::level::err;

struct Args {
    std::string path;
    std::string bins =
        "0k 4k 8k 16k 32k 64k 512k 1m 4m 8m 16m 32m 64m 128m 256m 512m 1g 4g "
        "8g 16g 32g 64g 128g 256g 512g 1t 2t 4t 8t 16t 32t 64t";
    bool do_stat;
    bool show_progress;
    int progress_interval = 10;
} args;

struct FStat {
    uint64_t dir_cnt;
    uint64_t file_cnt;
    uint64_t file_size;
    uint64_t sock_cnt;
    uint64_t unknown_cnt;
    uint64_t symlink_cnt;
    uint64_t chr_cnt ;
    uint64_t fifo_cnt;
    uint64_t blk_cnt;
    uint64_t skip_cnt;
    uint64_t zerofile_cnt;

    uint64_t total_file_cnt;
    uint64_t total_dir_cnt;
    uint64_t total_sock_cnt;
    uint64_t total_unknown_cnt;
    uint64_t total_symlink_cnt;
    uint64_t total_chr_cnt;
    uint64_t total_fifo_cnt;
    uint64_t total_blk_cnt;
    uint64_t total_skip_cnt;
    uint64_t total_zerofile_cnt;

    uint64_t total_file_size;
    uint64_t avg_file_size;
    uint64_t avg_dir_size;
    uint64_t max_dir;
    uint64_t max_file;
    uint64_t global_max_dir;
    uint64_t global_max_file;

    std::vector<uint64_t> hist_bins = {};
    std::vector<uint64_t> hist_fsize = {};
    std::vector<uint64_t> hist_fnum = {};
    std::vector<uint64_t> total_hist_fsize = {};
    std::vector<uint64_t> total_hist_fnum = {};

    double elapsed;

} fst = {};

static std::string validate_bins(const std::string& bins) {
    // parse bin information and push to bin vector
    // extra precaution: make sure the bin vector is sorted
    std::istringstream iss(bins);
    bool have_zero = false;
    
    do {
        std::string ele;
        iss >> ele;

        // iss will give me empty string at the end.        
        if (ele.empty()) break;

        size_t val = conv_size(ele);
        if (val == SIZE_MAX) {
            return "Bin size format error";
        } else if (val == 0) {
            have_zero = true;
        }
        fst.hist_bins.push_back(val);
        FP_LOG->debug("Finish validate: {}, val = {}", ele, val);
    } while (iss);

    if (!have_zero) {
        fst.hist_bins.push_back(0);  // always have 0 at the left end
    }
    fst.hist_bins.push_back(UINT64_MAX); // we have a catch all bin

    // in the end, we will sort them
    sort(fst.hist_bins.begin(), fst.hist_bins.end());

    // populate
    int buckets_cnt = fst.hist_bins.size();
    fst.hist_fnum.resize(buckets_cnt, 0);
    fst.hist_fsize.resize(buckets_cnt, 0);
    fst.total_hist_fnum.resize(buckets_cnt, 0);
    fst.total_hist_fsize.resize(buckets_cnt, 0);

    FP_LOG->debug("Total number of buckdets : {}", buckets_cnt);
    return "";
}

static inline int hist_idx(uint64_t val) {
    // given a value, return an index
    auto idx = std::lower_bound(fst.hist_bins.begin(), fst.hist_bins.end(), val) - fst.hist_bins.begin();
    return idx;
}

bool parse_args(int argc, char* argv[]) {
    // retval is a flag that indicates if there is any parsing error
    // there can be only one process doing the call of app.exit(e)
    // in this case, master does that. All other processes, in the event
    // of parse error, will set retval to a non -1.
    // All processes agree to if there is a parsing error base on if the
    // retval is -1.

    int retval = -1;

    app.set_help_all_flag("--help-all", "Expand all help");

    app.add_option("path", args.path, "Destination path")
        ->required()
        ->check(CLI::ExistingDirectory);

    app.add_flag("--progress", args.show_progress, "show progress");
    app.add_option("--interval", args.progress_interval, "progress interval");
    app.add_option("--bins", args.bins, "histogram bins")->check(validate_bins);

    try {
        app.parse(argc, argv);
    } catch (const CLI::ParseError& e) {
        if (rank == 0)
            retval = app.exit(e);
        else
            retval = 1;
    }
    return true ? retval == -1 : false;
}


static void reduce_hist() {
    // aggregate bin

    int buckets_cnt = fst.hist_bins.size();
    uint64_t* recvbuf = (uint64_t*)malloc(buckets_cnt * sizeof(uint64_t));

    MPI_Reduce(&fst.hist_fnum[0], recvbuf, buckets_cnt,
               MPI_UINT64_T, MPI_SUM, 0, MPI_COMM_WORLD);


    for (int i = 0; i < buckets_cnt; i++) {
        fst.total_hist_fnum[i] = recvbuf[i];
    }

    MPI_Reduce(fst.hist_fsize.data(), recvbuf, buckets_cnt,
               MPI_UINT64_T, MPI_SUM, 0, MPI_COMM_WORLD);

    for (int i = 0; i < buckets_cnt; i++) {
        fst.total_hist_fsize[i] = recvbuf[i];
    }

    free(recvbuf);
}

static void reduce_summary() {

    // gather results
    MPI_Reduce(&fst.dir_cnt, &fst.total_dir_cnt, 1, MPI_UINT64_T,
               MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&fst.file_cnt, &fst.total_file_cnt, 1, MPI_UINT64_T,
               MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&fst.chr_cnt, &fst.total_chr_cnt, 1, MPI_UINT64_T,
               MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&fst.sock_cnt, &fst.total_sock_cnt, 1, MPI_UINT64_T,
               MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&fst.fifo_cnt, &fst.total_fifo_cnt, 1, MPI_UINT64_T,
               MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&fst.unknown_cnt, &fst.total_unknown_cnt, 1, MPI_UINT64_T,
               MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&fst.blk_cnt, &fst.total_blk_cnt, 1, MPI_UINT64_T,
               MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&fst.symlink_cnt, &fst.total_symlink_cnt, 1, MPI_UINT64_T,
               MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&fst.max_file, &fst.global_max_file, 1,
               MPI_UINT64_T, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&fst.max_dir, &fst.global_max_dir, 1, MPI_UINT64_T,
               MPI_MAX, 0, MPI_COMM_WORLD);

    // aggregate file size
    MPI_Reduce(&fst.file_size, &fst.total_file_size, 1,
               MPI_UINT64_T, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&fst.zerofile_cnt, &fst.total_zerofile_cnt, 1, MPI_UINT64_T,
               MPI_SUM, 0, MPI_COMM_WORLD);

    reduce_hist();
}

static void output_summary() {
    long long total =
        fst.total_dir_cnt + fst.total_file_cnt + fst.symlink_cnt + fst.skip_cnt;
    std::cout << "\n\nScanned a total of " << format(total) << " entries, "
              << "elapsed time: " << format_time(fst.elapsed);
    std::cout << ", rate: " << (int)(total / fst.elapsed) << "/s \n\n";
}

static std::vector<std::string> trans_bucket_names() {
    std::vector<std::string> bucket_names;
    for (auto i : fst.hist_bins) {
        bucket_names.push_back("<= " + format_bytes(i, 0)); // precision is 0
    }
    bucket_names[0] = " = 0K";
    return bucket_names;
}

static void output_histogram() {
    using namespace std;
    cout << "\n\nFile system histogram\n\n";
    vector<string> bucket_names = trans_bucket_names();

    cout << setw(12) << left << "Buckets" << setw(20) << left
         << "Number of files" << setw(20) << left << "File size" << setw(20)
         << left << "%(Files)" << setw(20) << left << "%(Size)"
         << "\n\n";

    for (uint64_t i = 0; i < fst.hist_bins.size(); i++) {
        cout << setw(12) << left << bucket_names[i] << setw(20) << left
             << format(fst.total_hist_fnum[i]) << setw(20) << left
             << format_bytes(fst.total_hist_fsize[i]) << setw(20) << left
             << format_percent((double)fst.total_hist_fnum[i] / fst.total_file_cnt) << setw(20)
             << left << format_percent((double)fst.total_hist_fsize[i] / fst.total_file_size)
             << "\n";
    }
}

void epilog() {
    using namespace std;

    cout << "\n\n";
    cout << setw(30) << left << "Number of files:" << setw(15) << left
         << format(fst.total_file_cnt) << "\n";
    cout << setw(30) << left << "Number of directories:" << setw(15) << left
         << format(fst.total_dir_cnt) << "\n";
    cout << setw(30) << left << "Number of Sym links:" << setw(15) << left
         << format(fst.symlink_cnt) << "\n";
    cout << setw(30) << left << "Total file size:" << setw(15) << left
         << format_bytes(fst.total_file_size) << "\n";
    if (fst.file_cnt != 0) {
        cout << setw(30) << left << "Average file size:" << setw(15) << left
            << format_bytes(fst.total_file_size / fst.file_cnt) << "\n";
    }
    if (fst.dir_cnt != 0) {
        cout << setw(30) << left << "Average directory entries:" << setw(15) << left
            << format(fst.file_cnt / fst.dir_cnt) << "\n";
    }

    cout << setw(30) << left << "Max file size:" << setw(15) << left
         << format_bytes(fst.global_max_file) << "\n";
    cout << setw(30) << left << "Max directory entries:" << setw(15) << left
         << format(fst.global_max_dir) << "\n";

    output_histogram();
    output_summary();
}

Profile::Profile(int argc, char* argv[]) : Ring(argc, argv) {
}

void Profile::reduce_init(std::vector<unsigned long>& invec) {
    invec.push_back(m_local_work_cnt);
}

void Profile::reduce_finalize(const std::vector<unsigned long> outvec) {
    if (rank == 0) {
        FP_LOG->info("processed item count: {}", outvec[0]);
    }
}

void Profile::process_directory(std::string pth) {
    DIR* dir;
    struct dirent* ent;

    if ((dir = opendir(pth.data())) != NULL) {
        u_int64_t dircnt = 0;
        while ((ent = readdir(dir)) != NULL) {
            WorkItem wi;
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
                continue;
            std::string fullname = pth + "/" + std::string(ent->d_name);
            wi.size = fullname.size() + 1; // plus 1 for null terminator
            wi.buf = (char*)malloc(wi.size);
            strcpy(wi.buf, fullname.c_str());
            enqueue(wi);
            dircnt++;
        }
        closedir(dir);
        if (dircnt > fst.max_dir) fst.max_dir = dircnt;
    } else {
        FP_LOG->error("opendir(): {} -> {}", pth, strerror(errno));
    }
}



void Profile::create() {
    fst.dir_cnt++;
    process_directory(args.path);
}

static void process_file(size_t fsize) {
    fst.file_cnt++;
    fst.file_size += fsize;
    if (fsize == 0) {
        fst.zerofile_cnt++;
    } else if (fsize > fst.max_file) {
        fst.max_file = fsize;
    }
    int idx = hist_idx(fsize);
    fst.hist_fnum[idx]++;
    fst.hist_fsize[idx] += fsize;
}


void Profile::process() {
    m_local_work_cnt++;
    WorkItem wi = dequeue();
    auto pth = std::string(wi.buf);
    FP_LOG->debug("processing #{}", pth);
    free(wi.buf);

    struct stat sb;
    if (lstat(pth.c_str(), &sb) == -1) {
        FP_LOG->warn("stat: {} failed, {}", pth, strerror(errno));
        fst.skip_cnt++;
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
            fst.blk_cnt++;
            break;
        case S_IFCHR:
            fst.chr_cnt++;
            break;
        case S_IFDIR:
            fst.dir_cnt++;
            process_directory(pth);
            break;
        case S_IFIFO:
            fst.fifo_cnt++;
            break;
        case S_IFLNK:
            fst.symlink_cnt++;
            break;
        case S_IFREG:
            process_file(sb.st_size);
            break;
        case S_IFSOCK:
            fst.sock_cnt++;
            break;
        default:
            fst.unknown_cnt++;
    }
    // TODO: store fitem 
}



int main(int argc, char* argv[]) {
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    bool flag = parse_args(argc, argv);
    if (flag) {
        args.path = abs_path(args.path);
        if (args.path.empty()) exit_app(EXIT_SUCCESS);
    }

    if (!flag) exit_app(EXIT_SUCCESS);


    // setup logging
    if (const char* env_p = std::getenv("FPROF_LOG_LEVEL")) {
        if (!strcmp(env_p, "debug")) {
            FP_LOG_LEVEL = spdlog::level::debug;
        } else if (!strcmp(env_p, "info")) {
            FP_LOG_LEVEL = spdlog::level::info;
        } else if (!strcmp(env_p, "err")) {
            FP_LOG_LEVEL = spdlog::level::err;
        } else if (!strcmp(env_p, "critical")) {
            FP_LOG_LEVEL = spdlog::level::critical;
        }
    }
    FP_LOG->set_level(FP_LOG_LEVEL);

    double start = MPI_Wtime();
    Profile fprof = Profile(argc, argv);
    if (args.show_progress) {
        fprof.enable_reduce(args.progress_interval,
                            false);  // user defined reduce
    }

    if (app.count("--bins") == 0) validate_bins(args.bins);
    
    //setup_histogram();

    // kick off the walk
    fprof.begin();
    fprof.finalize();
    reduce_summary();
    double end = MPI_Wtime();
    fst.elapsed = end - start;
    if (rank == 0) {
        epilog();
    }
    MPI_Finalize();
    return 0;
}
