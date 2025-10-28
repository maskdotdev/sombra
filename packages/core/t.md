Issue #1: Version chain traversal not implemented

• Status: ✅ Known limitation, documented
• Code only reads head version, doesn't follow prev_version chain
• Comment at version_chain.rs:133 explicitly states this is deferred
• Impact: Medium priority, breaks historical reads but most apps want latest data

Issue #2: commit_ts==0 visibility logic

• Status: ✅ NOT a bug (initially appeared suspicious)
• The snapshot_ts == 0 condition is for legacy record backward compatibility
• Logic correctly handles both MVCC and non-MVCC modes
• Recommendation: Add clarifying comments

Issue #3: Edges created with tx_id=0 🔴 - FIXED

• Status: 🐛 CRITICAL BUG
• Location: src/db/core/edges.rs:52-60
• Edge creation passes tx_id=0 to store_new_version(), unlike nodes which pass actual tx_id
• Breaks read-your-own-writes for edges within transactions
• Fix: 5 lines - pass tx_id and commit_ts to add_edge_internal() like nodes do

Issue #4: Traversal snapshot isolation - FIXED

• Status: ⚠️ Partial issue
• Traversals use load_edge() not load_edge_with_snapshot()
• Edge list structure is snapshot-isolated (stored in node versions)
• But edge properties might show newer version data
• Fix: 10 lines - use snapshot-aware edge loading in traversals

Issue #5: Stale index entries on node update

• Status: ✅ Known limitation with TODO comment
• Location: src/db/core/nodes.rs:140-148
• Old labels/properties not removed from indexes on update
• Explicitly marked as TODO in code
• Impact: Medium-high priority, affects query correctness

Issue #6: No file locking 🔴

• Status: ❌ CRITICAL MISSING FEATURE - FIXED
• No inter-process file locking implemented
• Multiple processes can corrupt database
• Fix: 30 lines using fs2 crate for exclusive lock

Issue #7: API requires &mut -DONE

• Status: ✅ Intentional design choice
• All methods require &mut GraphDB, preventing concurrent access
• MVCC infrastructure exists but API doesn't expose concurrency
• Not a bug - valid single-threaded design
• Recommendation: Document this limitation
