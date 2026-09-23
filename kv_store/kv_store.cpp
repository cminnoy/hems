#include <csignal>
#include <cstdio>
#include <cstring>
#include <cassert>
#include <cstdlib>

#include <new>
#include <thread>
#include <iostream>
#include <iomanip>
#include <chrono>
#include <string>
#include <memory_resource>

#include <getopt.h>
#include <kv_store.hpp>

bool stop = false;
int exit_code = EXIT_SUCCESS;

std::pmr::unsynchronized_pool_resource memory_pool;
std::pmr::string component_name(&memory_pool);
std::pmr::string component_realm(&memory_pool);
std::pmr::string database_name(&memory_pool);
bool verbose = false;

void print_help() {
    std::cout << "Usage:\n"
                 "  kv_store -n|--name <component_name> [-v|--verbose]\n"
                 "Options:\n"
                 "  -n, --name <component_name>\n"
                 "      Specify the component name [mandatory]\n"
                 "  -r, --realm <component_realm>\n"
                 "      Specify the component realm [mandatory]\n"
                 "  -d, --database <sqlite3_database_name> [mandatory]\n"
                 "      Specify the name of the database for key/value storage\n"
                 "  -v, --verbose\n"
                 "      Enable verbose output\n"
                 "  -h, --help\n"
                 "      Display this help and exit\n";
    std::cout.flush();
}

void parse_args(int argc, char *argv[]) {
    static struct option long_options[] = {
        {"name", required_argument, nullptr, 'n'},
        {"realm", required_argument, nullptr, 'r'},
        {"database", required_argument, nullptr, 'd'},
        {"verbose", no_argument, nullptr, 'v'},
        {"help", no_argument, nullptr, 'h'},
        {0, 0, 0, 0}
    };

    int opt;
    while ((opt = getopt_long(argc, argv, "n:r:d:vh", long_options, nullptr)) != -1) {
        char * token;
        int address;
        switch (opt) {
            case 'n':
                component_name = optarg;
                break;
            case 'r':
                component_realm = optarg;
                break;
            case 'd':
                database_name = optarg;
                break;
            case 'v':
                verbose = true;
                break;
            case 'h':
                print_help();
                exit(EXIT_SUCCESS);
            default:
                std::cerr << "Error: Invalid option. Use -h for help.\n";
                exit(1);
        }
    }

    if (component_name.empty()) {
        std::cerr << "Error: Component name is mandatory. Use -h for help.\n";
        exit(EXIT_FAILURE);
    }
    if (component_realm.empty()) {
        std::cerr << "Error: Component realm is mandatory. Use -h for help.\n";
        exit(EXIT_FAILURE);
    }
    if (database_name.empty()) {
        std::cerr << "Error: Database name is mandatory. Use -h for help.\n";
        exit(EXIT_FAILURE);
    }
}

int main(int argc, char *argv[]) {
    // Handle arguments
    parse_args(argc, argv);

    // Random seed
    std::srand(std::time(nullptr));

    // Register 'break' handler
    std::signal(SIGINT, [](int) { stop = true; });
    std::signal(SIGTERM, [](int) { stop = true; });

    // Use polymorphic memory system for memory management
    std::pmr::set_default_resource(&memory_pool);

    // Execute component
    kv_store(&memory_pool, component_name, component_realm, database_name).run();

    // Cleanup
    std::signal(SIGINT, SIG_DFL);
    std::signal(SIGTERM, SIG_DFL);

    return exit_code;
}
