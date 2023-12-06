/*
 * KeyValueStoreMemory.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "fdbclient/BlobCipher.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/Notified.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbserver/DeltaTree.h"
#include "fdbclient/GetEncryptCipherKeys.h"
#include "fdbserver/IDiskQueue.h"
#include "fdbserver/IKeyValueContainer.h"
#include "fdbclient/IKeyValueStore.h"
#include "fdbserver/RadixTree.h"
#include "flow/ActorCollection.h"
#include "flow/EncryptUtils.h"
#include "flow/Knobs.h"
#include "fdbclient/S3BlobStore.h"

#include "flow/actorcompiler.h" // This must be the last #include.

#define OP_DISK_OVERHEAD (sizeof(OpHeader) + 1)
#define ENCRYPTION_ENABLED_BIT 31
static_assert(sizeof(uint32_t) == 4);

template <typename Container>
class KeyValueStoreMemory final : public IKeyValueStore, NonCopyable {
public:
	KeyValueStoreMemory(IDiskQueue* log,
	                    Reference<AsyncVar<ServerDBInfo> const> db,
	                    UID id,
	                    int64_t memoryLimit,
	                    KeyValueStoreType storeType,
	                    bool disableSnapshot,
	                    bool replaceContent,
	                    bool exactRecovery,
	                    bool enableEncryption);

	// IClosable
	Future<Void> getError() const override { return Void(); }
	Future<Void> onClosed() const override { return Void(); }

	void dispose() override {
		delete this;
	}
	void close() override {
		delete this;
	}

	// IKeyValueStore
	KeyValueStoreType getType() const override { return type; }

	void set(KeyValueRef keyValue, const Arena* arena) override {
		TraceEvent("SetInDDS3", id);
		queue.set(keyValue, arena);
	}

	void clear(KeyRangeRef range, const Arena* arena) override {
		queue.clear(range, arena);
	}

    ACTOR Future<Void> commitActor(KeyValueStoreMemory* self) {
        wait(commit_queue(self));
        return Void();
    }

	Future<Void> commit(bool sequential) override {
		TraceEvent("CommitInDDS3", id);
        return commitActor(this);
	}

    ACTOR Future<Optional<Value>> readValueActor(KeyValueStoreMemory* self, KeyRef key, Optional<ReadOptions> options) {
       std::string resource;
        std::string error;
        std::string blobstoreURI = "blobstore://minioadmin:minioadmin@127.0.0.1:9000?bucket=test-fdb-bucket&secure_connection=0&region=us-east-1";
        S3BlobStoreEndpoint::ParametersT parameters;
        Reference<S3BlobStoreEndpoint> s3 = S3BlobStoreEndpoint::fromString(blobstoreURI, {}, &resource, &error, &parameters);


        std::string value = wait(s3->readEntireFile("test-fdb-bucket", key.toString()));
        return Optional<Value>(ValueRef(value));
    }

	Future<Optional<Value>> readValue(KeyRef key, Optional<ReadOptions> options) override {
		TraceEvent("GetInDDS3", id);
        return readValueActor(this, key, options);
		//return Optional<Value>();
	}

	Future<Optional<Value>> readValuePrefix(KeyRef key, int maxLength, Optional<ReadOptions> options) override {
	    /*
        if (recovering.isError())
			throw recovering.getError();
		if (!recovering.isReady())
			return waitAndReadValuePrefix(this, key, maxLength, options);

		auto it = data.find(key);
		if (it == data.end())
			return Optional<Value>();
		auto val = it.getValue();
		if (maxLength < val.size()) {
			return Optional<Value>(val.substr(0, maxLength));
		} else {
			return Optional<Value>(val);
		}
        */
	    return Optional<Value>();
    }

	// If rowLimit>=0, reads first rows sorted ascending, otherwise reads last rows sorted descending
	// The total size of the returned value (less the last entry) will be less than byteLimit
	Future<RangeResult> readRange(KeyRangeRef keys,
	                              int rowLimit,
	                              int byteLimit,
	                              Optional<ReadOptions> options) override {
		/*
        if (recovering.isError())
			throw recovering.getError();
		if (!recovering.isReady())
			return waitAndReadRange(this, keys, rowLimit, byteLimit, options);

		RangeResult result;
		if (rowLimit == 0) {
			return result;
		}

		if (rowLimit > 0) {
			auto it = data.lower_bound(keys.begin);
			while (it != data.end() && rowLimit && byteLimit > 0) {
				StringRef tempKey = it.getKey(reserved_buffer);
				if (tempKey >= keys.end)
					break;

				byteLimit -= sizeof(KeyValueRef) + tempKey.size() + it.getValue().size();
				result.push_back_deep(result.arena(), KeyValueRef(tempKey, it.getValue()));
				++it;
				--rowLimit;
			}
		} else {
			rowLimit = -rowLimit;
			auto it = data.previous(data.lower_bound(keys.end));
			while (it != data.end() && rowLimit && byteLimit > 0) {
				StringRef tempKey = it.getKey(reserved_buffer);
				if (tempKey < keys.begin)
					break;

				byteLimit -= sizeof(KeyValueRef) + tempKey.size() + it.getValue().size();
				result.push_back_deep(result.arena(), KeyValueRef(tempKey, it.getValue()));
				it = data.previous(it);
				--rowLimit;
			}
		}

		result.more = rowLimit == 0 || byteLimit <= 0;
		return result;
        */
        throw not_implemented();
	}
StorageBytes getStorageBytes() const override {
        return StorageBytes((int64_t) 10000000000000, (int64_t) 10000000000000, (int64_t) 0, (int64_t) 10000000000000);
    }
	Future<EncryptionAtRestMode> encryptionMode() override {
		return EncryptionAtRestMode(EncryptionAtRestMode::DISABLED);
	}


private:
	enum OpType {
		OpSet,
		OpClear,
		OpClearToEnd,
		OpSnapshotItem,
		OpSnapshotEnd,
		OpSnapshotAbort, // terminate an in progress snapshot in order to start a full snapshot
		OpCommit, // only in log, not in queue
		OpRollback, // only in log, not in queue
		OpSnapshotItemDelta,
		OpEncrypted_Deprecated // deprecated since we now store the encryption status in the first bit of the opType
	};

	struct OpRef {
		OpType op;
		StringRef p1, p2;
		OpRef() {}
		OpRef(Arena& a, OpRef const& o) : op(o.op), p1(a, o.p1), p2(a, o.p2) {}
		size_t expectedSize() const { return p1.expectedSize() + p2.expectedSize(); }
	};
	struct OpHeader {
		uint32_t op;
		int len1, len2;
	};

	struct OpQueue {
		OpQueue() : numBytes(0) {}

		int totalSize() const { return numBytes; }

		void clear() {
			numBytes = 0;
			operations = Standalone<VectorRef<OpRef>>();
			arenas.clear();
		}

		void rollback() { clear(); }

		void set(KeyValueRef keyValue, const Arena* arena = nullptr) {
			queue_op(OpSet, keyValue.key, keyValue.value, arena);
		}

		void clear(KeyRangeRef range, const Arena* arena = nullptr) {
			queue_op(OpClear, range.begin, range.end, arena);
		}

		void clear_to_end(StringRef fromKey, const Arena* arena = nullptr) {
			queue_op(OpClearToEnd, fromKey, StringRef(), arena);
		}

		void queue_op(OpType op, StringRef p1, StringRef p2, const Arena* arena) {
			numBytes += p1.size() + p2.size() + sizeof(OpHeader) + sizeof(OpRef);

			OpRef r;
			r.op = op;
			r.p1 = p1;
			r.p2 = p2;
			if (arena == nullptr) {
				operations.push_back_deep(operations.arena(), r);
			} else {
				operations.push_back(operations.arena(), r);
				arenas.push_back(*arena);
			}
		}

		const OpRef* begin() { return operations.begin(); }

		const OpRef* end() { return operations.end(); }

	private:
		Standalone<VectorRef<OpRef>> operations;
		uint64_t numBytes;
		std::vector<Arena> arenas;
	};
	KeyValueStoreType type;
	UID id;

	Container data;
	// reserved buffer for snapshot/fullsnapshot
	uint8_t* reserved_buffer;

	OpQueue queue; // mutations not yet commit()ted
	IDiskQueue* log;
	Reference<AsyncVar<ServerDBInfo> const> db;
	Future<Void> recovering, snapshotting;
	int64_t committedWriteBytes;
	int64_t overheadWriteBytes;
	NotifiedVersion notifiedCommittedWriteBytes;
	Key recoveredSnapshotKey; // After recovery, the next key in the currently uncompleted snapshot
	IDiskQueue::location
	    currentSnapshotEnd; // The end of the most recently completed snapshot (this snapshot cannot be discarded)
	IDiskQueue::location previousSnapshotEnd; // The end of the second most recently completed snapshot (on commit, this
	                                          // snapshot can be discarded)
	PromiseStream<Future<Void>> addActor;
	Future<Void> commitActors;

	int64_t committedDataSize;
	int64_t transactionSize;
	bool transactionIsLarge;

	bool resetSnapshot; // Set to true after a fullSnapshot is performed.  This causes the regular snapshot mechanism to
	                    // restart
	bool disableSnapshot;
	bool replaceContent;
	bool firstCommitWithSnapshot;
	int snapshotCount;

	int64_t memoryLimit; // The upper limit on the memory used by the store (excluding, possibly, some clear operations)
	std::vector<std::pair<KeyValueMapPair, uint64_t>> dataSets;

	bool enableEncryption;
	TextAndHeaderCipherKeys cipherKeys;
	Future<Void> refreshCipherKeysActor;

	ACTOR static Future<Void> commit_queue(KeyValueStoreMemory* self) {
        state const OpRef* o;

        loop {
            o = self->queue.begin();
            if (o == self->queue.end()) {
                break;
            }

		//for (const OpRef* ; ; ++o) {
			if (o->op == OpSet) {
                /*
				if (sequential) {
					KeyValueMapPair pair(o->p1, o->p2);
					dataSets.emplace_back(pair, pair.arena.getSize() + data.getElementBytes());
				} else {
					data.insert(o->p1, o->p2);
				}
                */
       std::string resource;
        std::string error;
        std::string blobstoreURI = "blobstore://minioadmin:minioadmin@127.0.0.1:9000?bucket=test-fdb-bucket&secure_connection=0&region=us-east-1";
        S3BlobStoreEndpoint::ParametersT parameters;
        Reference<S3BlobStoreEndpoint> s3 = S3BlobStoreEndpoint::fromString(blobstoreURI, {}, &resource, &error, &parameters);
                wait(s3->writeEntireFile("test-fdb-bucket", o->p1.toString(), o->p2.toString()));
			} else if (o->op == OpClear) {
                /*
				if (sequential) {
					data.insert(dataSets);
					dataSets.clear();
				}
				data.erase(data.lower_bound(o->p1), data.lower_bound(o->p2));
                */
       std::string resource;
        std::string error;
        std::string blobstoreURI = "blobstore://minioadmin:minioadmin@127.0.0.1:9000?bucket=test-fdb-bucket&secure_connection=0&region=us-east-1";
        S3BlobStoreEndpoint::ParametersT parameters;
        Reference<S3BlobStoreEndpoint> s3 = S3BlobStoreEndpoint::fromString(blobstoreURI, {}, &resource, &error, &parameters);
                wait(s3->deleteObject("test-fdb-bucket", o->p1.toString()));
			} else
				ASSERT(false);
            ++o;
        }

		self->queue.clear();
		return Void();
	}


    /*
	ACTOR static Future<Optional<Value>> waitAndReadValue(KeyValueStoreMemory* self,
	                                                      Key key,
	                                                      Optional<ReadOptions> options) {
		wait(self->recovering);
		return static_cast<IKeyValueStore*>(self)->readValue(key, options).get();
	}
	ACTOR static Future<Optional<Value>> waitAndReadValuePrefix(KeyValueStoreMemory* self,
	                                                            Key key,
	                                                            int maxLength,
	                                                            Optional<ReadOptions> options) {
		wait(self->recovering);
		return static_cast<IKeyValueStore*>(self)->readValuePrefix(key, maxLength, options).get();
	}
	ACTOR static Future<RangeResult> waitAndReadRange(KeyValueStoreMemory* self,
	                                                  KeyRange keys,
	                                                  int rowLimit,
	                                                  int byteLimit,
	                                                  Optional<ReadOptions> options) {
		wait(self->recovering);
		return static_cast<IKeyValueStore*>(self)->readRange(keys, rowLimit, byteLimit, options).get();
	}
    */

    /*
	ACTOR static Future<Void> waitAndCommit(KeyValueStoreMemory* self, bool sequential) {
        std::string resource;
        std::string error;
        std::string blobstoreURI = "blobstore://minioadmin:minioadmin@127.0.0.1:9000?bucket=test-fdb-bucket&secure_connection=0&region=us-east-1";
        S3BlobStoreEndpoint::ParametersT parameters;
        Reference<S3BlobStoreEndpoint> s3 = S3BlobStoreEndpoint::fromString(blobstoreURI, {}, &resource, &error, &parameters);

		S3BlobStoreEndpoint::ListResult contents = wait(s3->listObjects("test-fdb-bucket"));
		std::vector<std::string> results;
		for (const auto& f : contents.objects) {
				TraceEvent("ListKeys", self->id).detail("name", f.name);
        }


	    TraceEvent(SevError, "S3SSInitSuccess", self->id).detail("Reason", "connected to S3");

		wait(self->recovering);
		wait(self->commit(sequential));
		return Void();
	}
    */
};

template <typename Container>
KeyValueStoreMemory<Container>::KeyValueStoreMemory(IDiskQueue* log,
                                                    Reference<AsyncVar<ServerDBInfo> const> db,
                                                    UID id,
                                                    int64_t memoryLimit,
                                                    KeyValueStoreType storeType,
                                                    bool disableSnapshot,
                                                    bool replaceContent,
                                                    bool exactRecovery,
                                                    bool enableEncryption)
  : type(storeType), id(id), log(log), db(db), committedWriteBytes(0), overheadWriteBytes(0), currentSnapshotEnd(-1),
    previousSnapshotEnd(-1), committedDataSize(0), transactionSize(0), transactionIsLarge(false), resetSnapshot(false),
    disableSnapshot(disableSnapshot), replaceContent(replaceContent), firstCommitWithSnapshot(true), snapshotCount(0),
    memoryLimit(memoryLimit), enableEncryption(enableEncryption) {
	/*
        this->reserved_buffer =
	    (storeType == KeyValueStoreType::MEMORY) ? nullptr : new uint8_t[CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT];
	if (this->reserved_buffer != nullptr)
		memset(this->reserved_buffer, 0, CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT);

	recovering = recover(this, exactRecovery);
	snapshotting = snapshot(this);
	commitActors = actorCollection(addActor.getFuture());
	if (enableEncryption) {
		refreshCipherKeysActor = refreshCipherKeys(this);
	}
    */
}

IKeyValueStore* keyValueStoreMemory(std::string const& basename,
                                    UID logID,
                                    int64_t memoryLimit,
                                    std::string ext,
                                    KeyValueStoreType storeType) {
	TraceEvent("KVSMemOpeningDD", logID)
	    .detail("Basename", basename)
	    .detail("MemoryLimit", memoryLimit)
	    .detail("StoreType", storeType);

    /*
	// SOMEDAY: update to use DiskQueueVersion::V2 with xxhash3 checksum for FDB >= 7.2
	IDiskQueue* log = openDiskQueue(basename, ext, logID, DiskQueueVersion::V1);
	if (storeType == KeyValueStoreType::MEMORY_RADIXTREE) {
		return new KeyValueStoreMemory<radix_tree>(
		    log, Reference<AsyncVar<ServerDBInfo> const>(), logID, memoryLimit, storeType, false, false, false, false);
	} else {
    */
		return new KeyValueStoreMemory<IKeyValueContainer>(
		    nullptr, Reference<AsyncVar<ServerDBInfo> const>(), logID, memoryLimit, storeType, false, false, false, false);
	/*}*/
}

IKeyValueStore* keyValueStoreLogSystem(class IDiskQueue* queue,
                                       Reference<AsyncVar<ServerDBInfo> const> db,
                                       UID logID,
                                       int64_t memoryLimit,
                                       bool disableSnapshot,
                                       bool replaceContent,
                                       bool exactRecovery,
                                       bool enableEncryption) {
	// ServerDBInfo is required if encryption is to be enabled, or the KV store instance have been encrypted.
	ASSERT(!enableEncryption || db.isValid());
	return new KeyValueStoreMemory<IKeyValueContainer>(queue,
	                                                   db,
	                                                   logID,
	                                                   memoryLimit,
	                                                   KeyValueStoreType::MEMORY,
	                                                   disableSnapshot,
	                                                   replaceContent,
	                                                   exactRecovery,
	                                                   enableEncryption);
}
