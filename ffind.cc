#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <regex>
#include <unordered_map>
#include <list>

#include <mpi.h>
#include "pop/ring.h"
#include "cli11.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"
#include "utils.h"
#include "flist.h"

using namespace utils;
using namespace POP;

// global 
auto FIND_LOG = spdlog::stdout_color_mt("find");
spdlog::level::level_enum FIND_LOG_LEVEL = spdlog::level::err;
CLI::App app{"Parallel Find"};

int size, rank;

struct args_t {
    std::string path;
    std::string sizeinfo;
    std::string name;
    std::string group;
    std::string user;
    std::string time;
    std::string stype;

    int type;
    int dirsize;

    bool size_greater;
    bool size_less;
    bool size_equal;
    bool do_delete;
    bool do_print;
    bool do_stat;
    size_t size_thres;
} args;

std::list<fitem_t> local_flist = {};


// predicates

inline bool size_equal_to(const fitem_t & fi) {
}

inline bool size_greater_than(const fitem_t & fi) {
}

inline bool size_less_than(const fitem_t & fi) {
}

inline bool type_is_dir(const fitem_t & fi) {
}

inline bool type_is_file(const fitem_t &fi) {
}

static std::string validate_size(const std::string &size) {
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

bool parse_args(int argc, char *argv[]) {
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
    app.add_option("--dirsize", args.dirsize, "by dir size");
    app.add_option("--name", args.name, "by name pattern");
    app.add_option("--time", args.time, "by time");  // newer or older
    app.add_option("--group", args.group, "by group");
    app.add_option("--user", args.user, "by user");
    app.add_set("--type", args.stype, {"link", "file", "dir"}, "by type");

    try {
        app.parse(argc, argv);
    } catch (const CLI::ParseError &e) {
        if (rank == 0)
            retval = app.exit(e);
        else
            retval = 1;
    }
    return true ? retval == -1 : false;
}

/**
 * check if we need to do stat based on the command line options.
 */
bool check_do_stat() {
    if (app.count("--size") > 0) return true;

    if (app.count("--time") > 0) return true;

    return false;
}


void output_summary() {}

void epilog() {
    using namespace std;
    output_summary();
}


int main(int argc, char* argv[]) {
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    bool flag = parse_args(argc, argv);
    if (flag) {
        args.do_stat = check_do_stat();

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
    }

    if (!flag) exit_app(EXIT_SUCCESS);

    // set up logging
    if (const char *env_p = std::getenv("FIND_LOG_LEVEL")) {
        if (!strcmp(env_p, "debug")) {
            FIND_LOG_LEVEL = spdlog::level::debug;
        } else if (!strcmp(env_p, "info")) {
            FIND_LOG_LEVEL = spdlog::level::info;
        } else if (!strcmp(env_p, "err")) {
            FIND_LOG_LEVEL = spdlog::level::err;
        } else if (!strcmp(env_p, "critical")) {
            FIND_LOG_LEVEL = spdlog::level::critical;
        }
    }
    FIND_LOG->set_level(FIND_LOG_LEVEL);

    Flist flist(args.path, rank, size);
    local_flist = flist.treewalk();
    FIND_LOG->debug("rank#{} local flish cnt = {}", rank, local_flist.size());
    if (rank == 0) {
        epilog();
    }
    MPI_Finalize();
    return 0;
}