#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <flatbuffers/flatbuffers.h>
#include <sqlite3.h>
#include <memory_resource>
#include <vector>
#include <string_view>

#include <fabrix/component.hpp>
#include <fabrix/endpoint.hpp>
#include <fabrix/message.hpp>
#include <schema/kv_store_generated.h>
#include "kv_store.hpp"

using namespace testing;
using namespace fabrix;

bool stop = false;
bool verbose = false;

namespace {

class MockPeerComponent : public fabrix::component {
public:
    using fabrix::component::component;

    MOCK_METHOD(void, on_command_response, (endpoint_type, double, std::string_view, std::int8_t, std::uint64_t, std::uint8_t const *, std::size_t, std::int32_t), (override));
    MOCK_METHOD(void, on_error, (endpoint_type, error_type), (override));

};

class testable_kv_store : public kv_store {
public:
    using kv_store::kv_store;
    using kv_store::on_command_request;
};

class KVStoreTestHarness {
public:
    static std::vector<std::uint8_t> build_put_payload(std::string_view key, double timestamp, std::vector<std::uint8_t> const& value) {
        flatbuffers::FlatBufferBuilder builder(1024);
        auto key_offset = builder.CreateString(key.data(), key.size());
        auto val_offset = builder.CreateVector(value.data(), value.size());

        auto kv_offset = CEMS::KVStore::CreateKV(builder, key_offset, val_offset, timestamp);
        builder.Finish(kv_offset);

        auto ptr = builder.GetBufferPointer();
        return std::vector<std::uint8_t>(ptr, ptr + builder.GetSize());
    }

    static std::vector<std::uint8_t> build_get_payload(std::string_view key, std::string_view source = "", std::string_view realm = "", double from = 0.0, double until = 0.0) {
        flatbuffers::FlatBufferBuilder builder(1024);
        auto key_offset = builder.CreateString(key.data(), key.size());
        auto src_offset = source.empty() ? 0 : builder.CreateString(source.data(), source.size());
        auto rlm_offset = realm.empty() ? 0 : builder.CreateString(realm.data(), realm.size());

        auto query_offset = CEMS::KVStore::CreateQuery(builder, key_offset, src_offset, rlm_offset, from, until);
        builder.Finish(query_offset);

        auto ptr = builder.GetBufferPointer();
        return std::vector<std::uint8_t>(ptr, ptr + builder.GetSize());
    }
};

class KVStoreTest : public Test {
protected:
    std::pmr::unsynchronized_pool_resource mem_pool;
    std::unique_ptr<testable_kv_store> store;
    std::unique_ptr<StrictMock<MockPeerComponent>> client;

    component::endpoint_type store_ep;
    component::endpoint_type client_ep;

    void SetUp() override {
        fabrix::endpoint::remove("gtest_kv_store_under_test");
        fabrix::endpoint::remove("gtest_client_peer");
        store = std::make_unique<testable_kv_store>(&mem_pool, "kv_store_under_test", "gtest", ":memory:", 65536);
        client = std::make_unique<StrictMock<MockPeerComponent>>(&mem_pool, "client_peer", "gtest", 65536);

        store_ep = store->public_endpoint();
        client_ep = client->public_endpoint();
    }

    void TearDown() override {
        store.reset();
        client.reset();
    }
};

// … includes and using-declarations unchanged …

TEST_F(KVStoreTest, PutCommandStoresDataAndReturnsSuccess) {
    std::vector<std::uint8_t> dummy_value = {0xDE, 0xAD, 0xBE, 0xEF};
    auto payload = KVStoreTestHarness::build_put_payload("sensor.temperature", 1719570000.0, dummy_value);

    EXPECT_CALL(*client, on_command_response(_, _, "put", 0, 42, nullptr, 0, 0))
        .Times(1);

    store->on_command_request(client_ep, client_ep, 1719570000.0, "put", 0, 42,
                              payload.data(), payload.size());
    client->process();
}

TEST_F(KVStoreTest, PutMalformedBufferTriggersError) {
    std::vector<std::uint8_t> broken_payload = {0x01, 0x02, 0x03, 0x04};

    // Malformed = programming error. No response is sent.
    // StrictMock guarantees on_command_response / on_error are NOT called.
    store->on_command_request(client_ep, client_ep, 1719570000.0, "put", 0, 101,
                              broken_payload.data(), broken_payload.size());
    client->process();
}

TEST_F(KVStoreTest, GetCommandRetrievesValidStoredEntries) {
    std::vector<std::uint8_t> initial_value = {0x11, 0x22, 0x33};
    auto put_payload = KVStoreTestHarness::build_put_payload("grid.voltage", 1719570005.0, initial_value);

    EXPECT_CALL(*client, on_command_response(_, _, "put", 0, 10, nullptr, 0, 0))
        .Times(1);
    store->on_command_request(client_ep, client_ep, 1719570005.0, "put", 0, 10,
                              put_payload.data(), put_payload.size());
    client->process();

    auto get_payload = KVStoreTestHarness::build_get_payload("grid.voltage");

    EXPECT_CALL(*client, on_command_response(_, _, "get", 0, 11, _, _, 0))
        .WillOnce(Invoke([](component::endpoint_type, double, std::string_view cmd,
                            std::int8_t, std::uint64_t, std::uint8_t const * res_data,
                            std::size_t res_size, std::int32_t result_code) {
            EXPECT_EQ(cmd, "get");
            EXPECT_EQ(result_code, 0);

            flatbuffers::Verifier verifier(res_data, res_size);
            ASSERT_TRUE(verifier.VerifyBuffer<CEMS::KVStore::Values>(nullptr));

            auto values_root = flatbuffers::GetRoot<CEMS::KVStore::Values>(res_data);
            ASSERT_EQ(values_root->values()->size(), 1);

            auto single_val = values_root->values()->Get(0);
            EXPECT_STREQ(single_val->source()->c_str(), "client_peer");
            EXPECT_STREQ(single_val->realm()->c_str(), "gtest");
            EXPECT_EQ(single_val->timestamp(), 1719570005.0);

            std::vector<std::uint8_t> extracted_blob(single_val->value()->begin(),
                                                      single_val->value()->end());
            EXPECT_THAT(extracted_blob, ElementsAre(0x11, 0x22, 0x33));
        }));

    store->on_command_request(client_ep, client_ep, 1719570006.0, "get", 0, 11,
                              get_payload.data(), get_payload.size());
    client->process();
}

TEST_F(KVStoreTest, GetCommandFiltersOutIncorrectTimeRange) {
    std::vector<std::uint8_t> dummy_value = {0x00};
    auto put_payload = KVStoreTestHarness::build_put_payload("battery.soc", 100.0, dummy_value);

    EXPECT_CALL(*client, on_command_response(_, _, _, _, 20, _, 0, 0))
        .WillOnce(Invoke([](component::endpoint_type, double, std::string_view cmd, std::int8_t, std::uint64_t, std::uint8_t const *, std::size_t, std::int32_t) {
            EXPECT_EQ(cmd, "put");
        }));
    store->on_command_request(client_ep, client_ep, 100.0, "put", 0, 20, put_payload.data(), put_payload.size());
    client->process();

    auto get_payload = KVStoreTestHarness::build_get_payload("battery.soc", "", "", 105.0, 200.0);

    EXPECT_CALL(*client, on_command_response(_, _, _, _, 21, _, _, -1))
        .WillOnce(Invoke([](component::endpoint_type, double, std::string_view cmd, std::int8_t, std::uint64_t, std::uint8_t const * res_data, std::size_t, std::int32_t result_code) {
            EXPECT_EQ(cmd, "get");
            EXPECT_EQ(result_code, -1);
            auto values_root = flatbuffers::GetRoot<CEMS::KVStore::Values>(res_data);
            EXPECT_EQ(values_root->values()->size(), 0);
        }));

    store->on_command_request(client_ep, client_ep, 106.0, "get", 0, 21, get_payload.data(), get_payload.size());
    client->process();
}

TEST_F(KVStoreTest, GetCommandReturnsEmptyOnNonExistentKey) {
    auto get_payload = KVStoreTestHarness::build_get_payload("missing.key");

    EXPECT_CALL(*client, on_command_response(_, _, _, _, 30, _, _, -1))
        .WillOnce(Invoke([](component::endpoint_type, double, std::string_view cmd, std::int8_t, std::uint64_t, std::uint8_t const * res_data, std::size_t, std::int32_t result_code) {
            EXPECT_EQ(cmd, "get");
            EXPECT_EQ(result_code, -1);
            auto values_root = flatbuffers::GetRoot<CEMS::KVStore::Values>(res_data);
            EXPECT_EQ(values_root->values()->size(), 0);
        }));

    store->on_command_request(client_ep, client_ep, 1719570000.0, "get", 0, 30, get_payload.data(), get_payload.size());
    client->process();
}

TEST_F(KVStoreTest, GetCommandFiltersBySource) {
    auto put = KVStoreTestHarness::build_put_payload("filter.key", 1000.0, {0xAA});
    EXPECT_CALL(*client, on_command_response(_, _, "put", 0, 1, nullptr, 0, 0)).Times(1);
    store->on_command_request(client_ep, client_ep, 1000.0, "put", 0, 1, put.data(), put.size());
    client->process();

    // source matches → 1 result
    auto get_match = KVStoreTestHarness::build_get_payload("filter.key", "client_peer");
    EXPECT_CALL(*client, on_command_response(_, _, "get", 0, 2, _, _, 0))
        .WillOnce(Invoke([](auto, auto, auto, auto, auto,
                            std::uint8_t const* d, std::size_t s, auto) {
            auto root = flatbuffers::GetRoot<CEMS::KVStore::Values>(d);
            EXPECT_EQ(root->values()->size(), 1);
        }));
    store->on_command_request(client_ep, client_ep, 1001.0, "get", 0, 2, get_match.data(), get_match.size());
    client->process();

    // source does NOT match → 0 results
    auto get_nomatch = KVStoreTestHarness::build_get_payload("filter.key", "other_component");
    EXPECT_CALL(*client, on_command_response(_, _, "get", 0, 3, _, _, -1))
        .WillOnce(Invoke([](auto, auto, auto, auto, auto,
                            std::uint8_t const* d, std::size_t s, auto) {
            auto root = flatbuffers::GetRoot<CEMS::KVStore::Values>(d);
            EXPECT_EQ(root->values()->size(), 0);
        }));
    store->on_command_request(client_ep, client_ep, 1002.0, "get", 0, 3, get_nomatch.data(), get_nomatch.size());
    client->process();
}

TEST_F(KVStoreTest, GetCommandReturnsMultipleEntries) {
    auto put1 = KVStoreTestHarness::build_put_payload("multi.key", 100.0, {0x01});
    auto put2 = KVStoreTestHarness::build_put_payload("multi.key", 200.0, {0x02, 0x03});

    EXPECT_CALL(*client, on_command_response(_, _, "put", 0, 1, nullptr, 0, 0)).Times(1);
    store->on_command_request(client_ep, client_ep, 100.0, "put", 0, 1, put1.data(), put1.size());
    client->process();

    EXPECT_CALL(*client, on_command_response(_, _, "put", 0, 2, nullptr, 0, 0)).Times(1);
    store->on_command_request(client_ep, client_ep, 200.0, "put", 0, 2, put2.data(), put2.size());
    client->process();

    auto get = KVStoreTestHarness::build_get_payload("multi.key");
    EXPECT_CALL(*client, on_command_response(_, _, "get", 0, 3, _, _, 0))
        .WillOnce(Invoke([](auto, auto, auto, auto, auto,
                            std::uint8_t const* d, std::size_t s, auto) {
            auto root = flatbuffers::GetRoot<CEMS::KVStore::Values>(d);
            ASSERT_EQ(root->values()->size(), 2);
            // verify both timestamps are present
            EXPECT_EQ(root->values()->Get(0)->timestamp(), 100.0);
            EXPECT_EQ(root->values()->Get(1)->timestamp(), 200.0);
        }));
    store->on_command_request(client_ep, client_ep, 300.0, "get", 0, 3, get.data(), get.size());
    client->process();
}

TEST_F(KVStoreTest, PutReplacesExistingEntry) {
    auto put1 = KVStoreTestHarness::build_put_payload("repl.key", 500.0, {0x11});
    EXPECT_CALL(*client, on_command_response(_, _, "put", 0, 1, nullptr, 0, 0)).Times(1);
    store->on_command_request(client_ep, client_ep, 500.0, "put", 0, 1, put1.data(), put1.size());
    client->process();

    // same key + same timestamp → REPLACE
    auto put2 = KVStoreTestHarness::build_put_payload("repl.key", 500.0, {0x22, 0x33});
    EXPECT_CALL(*client, on_command_response(_, _, "put", 0, 2, nullptr, 0, 0)).Times(1);
    store->on_command_request(client_ep, client_ep, 500.0, "put", 0, 2, put2.data(), put2.size());
    client->process();

    auto get = KVStoreTestHarness::build_get_payload("repl.key");
    EXPECT_CALL(*client, on_command_response(_, _, "get", 0, 3, _, _, 0))
        .WillOnce(Invoke([](auto, auto, auto, auto, auto,
                            std::uint8_t const* d, std::size_t s, auto) {
            auto root = flatbuffers::GetRoot<CEMS::KVStore::Values>(d);
            ASSERT_EQ(root->values()->size(), 1);   // replaced, not duplicated
            auto val = root->values()->Get(0);
            std::vector<std::uint8_t> blob(val->value()->begin(), val->value()->end());
            EXPECT_THAT(blob, ElementsAre(0x22, 0x33));  // new value, not old
        }));
    store->on_command_request(client_ep, client_ep, 600.0, "get", 0, 3, get.data(), get.size());
    client->process();
}

TEST_F(KVStoreTest, GetMalformedBufferTriggersError) {
    std::vector<std::uint8_t> broken = {0xFF, 0xFE, 0xFD};
    // Programming error → no response, StrictMock guarantees nothing is delivered
    store->on_command_request(client_ep, client_ep, 1000.0, "get", 0, 99, broken.data(), broken.size());
    client->process();
}

TEST_F(KVStoreTest, UnknownCommandIsIgnored) {
    std::vector<std::uint8_t> dummy = {0x01};
    // "delete" is neither "put" nor "get" → falls through, no response
    store->on_command_request(client_ep, client_ep, 1000.0, "delete", 0, 50, dummy.data(), dummy.size());
    client->process();
}

} // namespace
