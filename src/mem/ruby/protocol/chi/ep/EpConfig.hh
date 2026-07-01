#ifndef EP_CONFIG_HH
#define EP_CONFIG_HH

#include <cstdlib>

inline int epNumNodesFromEnv()
{
    const char *e = std::getenv("UBCC_NUM_NODES");
    if (e) {
        int n = std::atoi(e);
        if (n >= 1 && n <= 16) return n;
    }
    return 3;
}

inline int epNumSocketsFromEnv()
{
    const char *e = std::getenv("UBCC_NUM_SOCKETS");
    if (e) {
        int s = std::atoi(e);
        if (s >= 1 && s <= 8) return s;
    }
    return 1;
}

#endif
