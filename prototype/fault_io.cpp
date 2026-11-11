// Linux-only C2 fixture. Loaded only into an isolated loopback test process.
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <sqlite3.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

static bool mode(const char *name) {
    const char *selected = std::getenv("SBMDR_IO_FAULT");
    return selected && std::strcmp(selected, name) == 0;
}
static bool raw_fd(int fd, bool data = false) {
    char link[64], path[4096];
    std::snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
    auto count = readlink(link, path, sizeof(path) - 1);
    if (count < 0)
        return false;
    path[count] = 0;
    struct stat stat{};
    return std::strstr(path, "sbmdr-fault-") && std::strstr(path, "/data/active/") &&
           (!data || (fstat(fd, &stat) == 0 && stat.st_size > 1024));
}
extern "C" ssize_t writev(int fd, const iovec *vectors, int count) {
    static auto real = reinterpret_cast<decltype(&writev)>(dlsym(RTLD_NEXT, "writev"));
    static bool fired = false;
    if (!fired && raw_fd(fd)) {
        if (mode("torn_frame") && raw_fd(fd, true)) {
            const iovec torn{vectors[0].iov_base, 7};
            real(fd, &torn, 1);
            _exit(86);
        }
        if (mode("eintr")) {
            fired = true;
            errno = EINTR;
            return -1;
        }
        if (mode("short")) {
            fired = true;
            const iovec short_write{vectors[0].iov_base, 7};
            return real(fd, &short_write, 1);
        }
        if (mode("enospc")) {
            fired = true;
            errno = ENOSPC;
            return -1;
        }
    }
    return real(fd, vectors, count);
}
extern "C" int fsync(int fd) {
    static auto real = reinterpret_cast<decltype(&fsync)>(dlsym(RTLD_NEXT, "fsync"));
    if (raw_fd(fd, true)) {
        if (mode("fsync_eio")) {
            errno = EIO;
            return -1;
        }
        if (mode("after_raw_fsync")) {
            const int result = real(fd);
            if (result == 0)
                _exit(86);
            return result;
        }
    }
    return real(fd);
}
extern "C" int syncfs(int fd) {
    static auto real = reinterpret_cast<decltype(&syncfs)>(dlsym(RTLD_NEXT, "syncfs"));
    if (raw_fd(fd) && mode("syncfs_eio")) {
        errno = EIO;
        return -1;
    }
    return real(fd);
}
extern "C" int rename(const char *before, const char *after) {
    static auto real = reinterpret_cast<decltype(&rename)>(dlsym(RTLD_NEXT, "rename"));
    const bool fixture = std::strstr(after, "sbmdr-fault-");
    const bool sealed =
        fixture && std::strstr(after, ".bmdr.zst") && !std::strstr(after, ".partial");
    const bool manifest =
        fixture && std::strstr(after, ".manifest.json") && !std::strstr(after, ".partial");
    if (sealed && mode("before_sealed_rename"))
        _exit(86);
    if (sealed && mode("slow_seal"))
        usleep(80000);
    if (sealed && mode("stalled_seal")) {
        static bool stalled = false;
        if (!stalled) {
            stalled = true;
            usleep(3500000);
        }
    }
    const int result = real(before, after);
    if (result == 0 &&
        ((sealed && mode("after_sealed_rename")) || (manifest && mode("after_manifest_rename"))))
        _exit(86);
    return result;
}
extern "C" int sqlite3_exec(sqlite3 *db, const char *sql,
                            int (*callback)(void *, int, char **, char **), void *arg,
                            char **error) {
    using Function = decltype(&sqlite3_exec);
    static auto real = reinterpret_cast<Function>(dlsym(RTLD_NEXT, "sqlite3_exec"));
    bool sealed = false;
    if (mode("after_catalog_commit") && std::strcmp(sql, "COMMIT") == 0) {
        real(
            db, "SELECT count(*) FROM chunks WHERE state='SEALED'",
            [](void *out, int, char **row, char **) {
                *static_cast<bool *>(out) = row[0] && std::atoi(row[0]) > 0;
                return 0;
            },
            &sealed, nullptr);
    }
    const int result = real(db, sql, callback, arg, error);
    if (result == SQLITE_OK && sealed)
        _exit(86);
    return result;
}
