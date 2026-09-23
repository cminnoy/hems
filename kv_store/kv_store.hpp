#pragma once

#ifndef INCLUDE_KV_STORE_HPP
#define INCLUDE_KV_STORE_HPP

#include <fabrix/fabrix.hpp>
#include <schema/kv_store_generated.h>
#include <sqlite3.h>
#include <unistd.h>

extern bool stop;
extern bool verbose;

class kv_store : public fabrix::component {
public:

    kv_store(std::pmr::memory_resource * const memory_resource,
             std::string_view name,
             std::string_view realm,
             std::string_view database,
             std::size_t const size = 65536)
    : fabrix::component(memory_resource, name, realm, size)
    {
        if (sqlite3_open(std::string{database}.c_str(), &database_) == SQLITE_OK) {
            initialize_database();
        }
    }

    ~kv_store() noexcept override {
        cleanup();
    }

    void run() {
        try {
            constexpr auto timestep = std::chrono::seconds(1);
            auto next_timepoint = std::chrono::steady_clock::now() + timestep;
            do {
                process_until(next_timepoint);
                while (next_timepoint < std::chrono::steady_clock::now()) next_timepoint += timestep;
            } while (!stop);
        } catch (std::exception const & e) {
            std::cerr << "Run loop error: " << e.what() << std::endl;
        }
    }

protected:

    void on_start() override {
        std::clog << "Component " << identifier().realm() << "::" << identifier().name() << " is online with pid " << getpid() << ".\n";
    }

    void on_command_request(endpoint_type sender_endpoint,
                            endpoint_type delivery_endpoint,
                            double timestamp,
                            std::string_view command,
                            std::int8_t priority,
                            std::uint64_t cr_identifier,
                            std::uint8_t const * data,
                            std::size_t size) override {
        flatbuffers::Verifier verifier(data, size);

        if (verbose) {
            std::cout << "Received command " << command << " from " << sender_endpoint.identifier().name() <<  " with priority " << static_cast<std::uint32_t>(priority) << '.' << std::endl;
        }

        if (command == "put") {
            if (verifier.VerifyBuffer<CEMS::KVStore::KV>(nullptr)) {
                auto start = std::chrono::system_clock::now();

                CEMS::KVStore::KV const * const kv = flatbuffers::GetRoot<CEMS::KVStore::KV>(data);

                std::string sender_name {sender_endpoint.identifier().name()};
                std::string sender_realm {sender_endpoint.identifier().realm()};
                std::int32_t result_code = -1;

                std::int64_t const realm_id = get_or_create_id("realms", sender_realm);
                std::int64_t const source_id = get_or_create_id("sources", sender_name);

                const char * const insert_sql = "INSERT OR REPLACE INTO kv_pairs (key, timestamp, source_id, realm_id, value) VALUES (?, ?, ?, ?, ?);";
                sqlite3_stmt* stmt = nullptr;

                if (sqlite3_prepare_v2(database_, insert_sql, -1, &stmt, nullptr) == SQLITE_OK) {
                    sqlite3_bind_text(stmt, 1, kv->key()->c_str(), -1, SQLITE_STATIC);
                    sqlite3_bind_double(stmt, 2, kv->timestamp());
                    sqlite3_bind_int64(stmt, 3, source_id);
                    sqlite3_bind_int64(stmt, 4, realm_id);
                    sqlite3_bind_blob(stmt, 5, kv->value()->data(), kv->value()->size(), SQLITE_STATIC);

                    sqlite3_step(stmt);
                    sqlite3_finalize(stmt);
                    result_code = 0;
                }

                // Return confirmation response, 0 means success
                response(delivery_endpoint, command, priority, cr_identifier, result_code);

                auto end = std::chrono::system_clock::now();
                if (verbose)
                    std::cout << "Inserted key-value pair in " << std::chrono::duration_cast<std::chrono::microseconds>(end -start).count() << " microseconds" << std::endl;
            } else {
                on_error(sender_endpoint, error_type::TopicMalformed);
            }
        } 
        else if (command == "get") {
            if (verifier.VerifyBuffer<CEMS::KVStore::Query>(nullptr)) {
                CEMS::KVStore::Query const * const query = flatbuffers::GetRoot<CEMS::KVStore::Query>(data);

                flatbuffers::String const * const key = query->key();
                flatbuffers::String const * const source = query->source();
                flatbuffers::String const * const realm = query->realm();
                double const from_ts = query->from();
                double const until_ts = query->until();

                // Construct selective narrow filtering query dynamically
                std::string sql = "SELECT kv.timestamp, s.name, r.name, kv.value FROM kv_pairs kv "
                                  "JOIN sources s ON kv.source_id = s.id "
                                  "JOIN realms r ON kv.realm_id = r.id "
                                  "WHERE kv.key = ?";

                if (source && source->size() > 0) sql += " AND s.name = ?";
                if (realm && realm->size() > 0)   sql += " AND r.name = ?";
                if (from_ts > 0.0)                sql += " AND kv.timestamp >= ?";
                if (until_ts > 0.0)               sql += " AND kv.timestamp <= ?";

                sqlite3_stmt* stmt = nullptr;
                int32_t result_code = -1;

                std::size_t const expected_topic_length = 4096;
                std::pmr::polymorphic_allocator<std::uint8_t> pa(memory_resource());
                fabrix::flatbuffers_allocator<std::pmr::polymorphic_allocator<std::uint8_t>> fbs_allocator(pa);
                flatbuffers::FlatBufferBuilder builder(expected_topic_length, &fbs_allocator);

                std::vector<flatbuffers::Offset<CEMS::KVStore::Value>> value_offsets;

                if (sqlite3_prepare_v2(database_, sql.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
                    int bind_idx = 1;
                    sqlite3_bind_text(stmt, bind_idx++, key->c_str(), -1, SQLITE_STATIC);

                    if (source && source->size() > 0) sqlite3_bind_text(stmt, bind_idx++, source->c_str(), -1, SQLITE_STATIC);
                    if (realm && realm->size() > 0)   sqlite3_bind_text(stmt, bind_idx++, realm->c_str(), -1, SQLITE_STATIC);
                    if (from_ts > 0.0)                sqlite3_bind_double(stmt, bind_idx++, from_ts);
                    if (until_ts > 0.0)               sqlite3_bind_double(stmt, bind_idx++, until_ts);

                    while (sqlite3_step(stmt) == SQLITE_ROW) {
                        result_code = 0; // Found entries
                        double r_ts = sqlite3_column_double(stmt, 0);
                        const char* r_source = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
                        const char* r_realm = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2));
                        const void* r_blob = sqlite3_column_blob(stmt, 3);
                        int r_bytes = sqlite3_column_bytes(stmt, 3);

                        auto res_source = r_source ? builder.CreateString(r_source) : 0;
                        auto res_realm = r_realm ? builder.CreateString(r_realm) : 0;
                        auto res_value = builder.CreateVector(reinterpret_cast<const uint8_t*>(r_blob), r_bytes);

                        value_offsets.push_back(CEMS::KVStore::CreateValue(builder, r_ts, res_source, res_realm, res_value));
                    }
                    sqlite3_finalize(stmt);
                }

                // Finalize layout payload matching your FlatBuffers schema layout requirements
                auto vec_offset = builder.CreateVector(value_offsets);
                auto payload = CEMS::KVStore::CreateValues(builder, vec_offset);
                builder.Finish(payload);

                if (!response(delivery_endpoint, command, priority, cr_identifier, builder.GetBufferPointer(), builder.GetSize(), result_code)) {
                    on_error(sender_endpoint, error_type::CommandResponseUndeliverable);
                }
            } else {
                on_error(sender_endpoint, error_type::TopicMalformed);
            }
        }
    }

    void on_error(endpoint_type endpoint, error_type error_code) override {
        std::cerr << "Error: '" << (endpoint ? endpoint.identifier().name() : "<>") << "' with error code " << error_code << '\n';
    }

    bool on_halt_component_request(MAYBE_UNUSED endpoint_type sender_endpoint) override {
        stop = true;
        return true;
    }

    void on_list_commands_request(MAYBE_UNUSED endpoint_type sender_endpoint, list_type<string_type> & commands) override {
        commands.emplace_back("put");
        commands.emplace_back("get");
    }

private:

    void initialize_database() {
        const char* schema = 
            "PRAGMA journal_mode=WAL;"
            "CREATE TABLE IF NOT EXISTS realms (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE);"
            "CREATE TABLE IF NOT EXISTS sources (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE);"
            "CREATE TABLE IF NOT EXISTS kv_pairs (key TEXT, timestamp REAL, source_id INTEGER, realm_id INTEGER, value BLOB, PRIMARY KEY (key, timestamp));";
        sqlite3_exec(database_, schema, nullptr, nullptr, nullptr);
    }

    std::int64_t get_or_create_id(const std::string& table, const std::string& name) {
        std::string insert_sql = "INSERT OR IGNORE INTO " + table + " (name) VALUES (?);";
        sqlite3_stmt* stmt = nullptr;
        if (sqlite3_prepare_v2(database_, insert_sql.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, name.c_str(), -1, SQLITE_STATIC);
            sqlite3_step(stmt);
            sqlite3_finalize(stmt);
        }

        std::string select_sql = "SELECT id FROM " + table + " WHERE name = ?;";
        std::int64_t id = -1;
        if (sqlite3_prepare_v2(database_, select_sql.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, name.c_str(), -1, SQLITE_STATIC);
            if (sqlite3_step(stmt) == SQLITE_ROW) {
                id = sqlite3_column_int64(stmt, 0);
            }
            sqlite3_finalize(stmt);
        }
        return id;
    }

    void cleanup() {
        if (database_) {
            sqlite3_close(database_);
            database_ = nullptr;
        }
    }

    sqlite3 * database_ = nullptr;
};

#endif /* INCLUDE_KV_STORE_HPP */