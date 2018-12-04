#pragma once

#include <mpi.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <chrono>  // chrono::system_clock
#include <ctime>   // localtime
#include <iomanip> // put_time
#include <locale>
#include <sstream> // stringstream
#include <string>  // string
#include <tuple>
#include <cmath>
#include <algorithm>

#ifdef HAVE_REGEX
#include <regex>
#endif

#include <iostream>


#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"

namespace utils
{

auto UTIL_LOG = spdlog::stdout_color_mt("util");

const uint64_t KiB = (uint64_t)1 << 10;
const uint64_t MiB = (uint64_t)1 << 20;
const uint64_t GiB = (uint64_t)1 << 30;
const uint64_t TiB = (uint64_t)1 << 40;
const uint64_t PiB = (uint64_t)1 << 50;
const uint64_t EiB = (uint64_t)1 << 60;

const uint64_t KB = pow(10, 3);
const uint64_t MB = pow(10, 6);
const uint64_t GB = pow(10, 9);
const uint64_t TB = pow(10, 12);
const uint64_t PB = pow(10, 15);
const uint64_t EB = pow(10, 18);

std::tuple<int, int, int> conv_time(long long t)
{
    int hour = t / 3600;
    int min = (t % 3600) / 60;
    int sec = (t % 3600) % 60;
    return std::make_tuple(hour, min, sec);
}


std::string format_time(const double time)
{
    std::stringstream ss;
    auto t = (long long)time;
    auto tri = conv_time(t);
    int hour = std::get<0>(tri);
    int min = std::get<1>(tri);
    int sec = std::get<2>(tri);

    if (hour != 0)
        ss << hour << " hrs";

    if (min != 0)
        ss << " " << min << " mins";

    if (sec != 0)
        ss << " " << sec << " secs";

    if (hour == 0 && min == 0 && sec == 0)
        ss << time << " secs";

    return ss.str();
}

template <typename T>
inline std::string format(const T &value)
{
    static std::locale loc("");
    std::stringstream ss;
    ss.imbue(loc);
    ss << value;
    return ss.str();
}

template <>
inline std::string format(const double &value)
{
    static std::locale loc("");
    std::stringstream ss;
    ss.imbue(loc);
    ss << std::fixed << std::setprecision(1) << value;
    return ss.str();
}

inline std::string format_percent(const double &value, int preci=2) {
    static std::locale loc("");
    std::stringstream ss;
    ss.imbue(loc);
    ss << std::fixed << std::setprecision(preci) << (value * 100) << "%";
    return ss.str();
}

// std::string current_time_and_date()
// {
//     auto now = std::chrono::system_clock::now();
//     auto in_time_t = std::chrono::system_clock::to_time_t(now);

//     std::stringstream ss;
//     ss << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d %X");
//     return ss.str();
// }

/** convert a number + unit to a number */
size_t conv_size(int n, char unit='C')
{
    size_t KB = 1 << 10;
    size_t MB = 1 << 20;
    size_t GB = (size_t)1 << 30;
    size_t TB = (size_t)1 << 40;
    size_t val = 0;

    switch (unit)
    {
    case 'C':
    case 'c':
        val = n;
        break;
    case 'K':
    case 'k':
        val = n * KB;
        break;
    case 'M':
    case 'm':
        val = n * MB;
        break;
    case 'G':
    case 'g':
        val = n * GB;
        break;
    case 'T':
    case 't':
        val = n * TB;
        break;
    default:
        val = -1;
    }
    return val;
}

#ifdef HAVE_REGEX
size_t conv_size(std::string s) {
    std::regex patt(R"((\d+)([ckmgt]+))", std::regex_constants::icase);
    std::smatch match;
    if (regex_search(s, match, patt)) {
        char unit = toupper(match[2].str().at(0));        
        return conv_size(std::stoi(match[1].str()), unit);
    }
    return -1;
}
#else 
size_t conv_size(std::string s) {
    // we don't have working regex
    char unit = s.back(); // last char
    std::vector<char> unitvec = {'c', 'C', 'k', 'K', 'm', 'M', 'g', 'G', 't', 'T'};
    if (std::find(unitvec.begin(), unitvec.end(), unit) != unitvec.end()) {
        std::string digits = s.substr(0, s.find(unit));
        return conv_size(std::stoi(digits), unit);
    } 
    // if here, it means we found no proper unit char
    // SIZE_MAX indicts error
    return SIZE_MAX; 
}
#endif

std::string dtype_str(int d_type)
{
    switch (d_type)
    {
    case DT_DIR:
        return "DT_DIR";
        break;
    case DT_REG:
        return "DT_REG";
        break;
    case DT_BLK:
        return "DT_BLK";
        break;
    case DT_CHR:
        return "DT_CHR";
        break;
    case DT_FIFO:
        return "DT_FIFO";
        break;
    case DT_LNK:
        return "DT_LNK";
        break;
    case DT_SOCK:
        return "DT_SOCK";
        break;
    case DT_UNKNOWN:
        return "DT_UNKNOWN";
        break;
    default:
        return "SUPER UNKNOWN";
    }
}

std::string abs_path(const std::string &path)
{
    // make path a absolute one.
    char buf[PATH_MAX + 1];
    char *res = realpath(path.data(), buf);
    if (res)
    {
        return std::string(buf);
    }
    else
    {
        UTIL_LOG->error("abs_path: {}", strerror(errno));
        return "";
    }
}

void exit_app(int code)
{
    MPI_Finalize();
    exit(code);
}

/**
 * covert a byte number to a human-readable string
 */
std::string format_bytes(const uint64_t size, int precison=2)
{
    using namespace std;
    stringstream ss;

    uint64_t eb = size / EiB;
    uint64_t pb = (size % EiB) / PiB;
    uint64_t tb = (size % EiB % PiB) / TiB;
    uint64_t gb = (size % EiB % PiB % TiB) / GiB;
    uint64_t mb = (size % EiB % PiB % TiB % GiB) / MiB;
    uint64_t kb = (size % EiB % PiB % TiB % GiB % MiB) / KiB;

    ss << fixed << setprecision(precison);

    if (eb != 0)
    {
        ss << (double)size / EiB << " EiB";
    }
    else if (pb != 0)
    {
        ss << (double)size / PiB << " PiB";
    }
    else if (tb != 0)
    {
        ss << (double)size / TiB << " TiB";
    }
    else if (gb != 0)
    {
        ss << (double)size / GiB << " GiB";
    }
    else if (mb != 0)
    {
        ss << (double)size / MiB << " MiB";
    }
    else if (kb != 0)
    {
        ss << (double)size / KiB << " KiB";
    }
    else
    {
        ss << size << " bytes";
    }
    return ss.str();
}


} // namespace utils
