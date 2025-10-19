Of course. Let's lay out a detailed, robust plan in natural language. We'll focus on the concepts, the design decisions, and the logical progression from a simple MVP to a powerful, project-ready database, all using Rust.

---

### **Project Codename: "Graphite"**

Our goal is to build a simple, file-based graph database inspired by SQLite's single-file architecture. It will be built in Rust, prioritizing correctness, performance, and a clear path for future growth.

### **Phase 0: The Blueprint and Core Concepts**

Before we write a single line of I/O code, we must define what we are building. This phase is about establishing the conceptual model.

**1. The Graph Model: The Property Graph**
We will adopt the popular **Property Graph Model**. This is an intuitive model where our world consists of:
*   **Nodes:** These are the entities or objects. A node could be a user, a file in a codebase, a function declaration, or a product.
*   **Edges (or Relationships):** These are the connections between nodes. They are directed and have a type, signifying the nature of the relationship, like `CALLS`, `IMPORTS`, or `FRIENDS_WITH`.
*   **Properties:** This is the data. Both nodes and edges can have a flexible set of key-value pairs, like `name: "main.rs"` or `since: 2021`.

**2. The In-Memory Data Structures (The "Soul" of the Database)**
We'll define Rust structs that represent our graph elements. The fields in these structs are critical as they will dictate our on-disk format and capabilities.

*   **The `Node` Structure:**
    *   `id`: A unique 64-bit number identifying this node forever.
    *   `labels`: A list of strings, like tags, that classify the node (e.g., `["File", "RustSource"]`). This is for coarse-grained searching.
    *   `properties`: A flexible map for storing key-value data.
    *   **`first_outgoing_edge_id`**: This is the most important "graph" field. It acts as a pointer, holding the ID of the first edge that starts *from* this node.
    *   **`first_incoming_edge_id`**: Similarly, this points to the ID of the first edge that points *to* this node.

*   **The `Edge` Structure:**
    *   `id`: A unique 64-bit number for the edge itself.
    *   `source_node_id` & `target_node_id`: The IDs of the nodes it connects.
    *   `type`: A string describing the relationship (e.g., "CALLS").
    *   `properties`: Its own key-value data.
    *   **`next_outgoing_edge_id`**: A pointer to the *next* edge that also starts from the same source node.
    *   **`next_incoming_edge_id`**: A pointer to the *next* edge that also points to the same target node.

**The Key Insight:** These `first_` and `next_` fields together create multiple **on-disk linked lists**. For any given node, you can find its first outgoing edge, and from that edge, you can hop to the next, and the next, until you have found all of its neighbors. This is the core mechanism that allows for fast graph traversal without needing a separate index for the MVP.

---

### **Phase 1: The Foundation - The Physical Storage Layer**

This phase is about how we physically arrange bytes in a single file on disk.

**1. The Database File: A Digital Book**
Our database will be a single file. This file is structured like a book, divided into fixed-size **Pages** (e.g., 8KB each). All reading and writing will happen in terms of these whole pages.

**2. The File Header (Page 0): The Table of Contents**
The very first page of the file is special. It's the **Header**, containing critical metadata about the database:
*   A "magic number" to identify the file as a "Graphite" database.
*   A version number for our file format, so we can handle upgrades later.
*   The page size used in this database.
*   The **global counters** for the `next_available_node_id` and `next_available_edge_id`.

**3. Storing Records on a Page**
A "record" is a serialized Node or Edge. When we write a record to a page, we won't just dump the bytes. We will prefix it with a tiny record header containing its `type` (Node or Edge) and its `size`. This lets us walk through a page and easily identify where each record begins and ends.

---

### **Phase 2: The Machinery - The Pager and Cache**

This is the engine's core, managing the flow of data between the slow disk and fast memory.

**1. The Pager: The Librarian**
The Pager's job is to fulfill requests for pages. When another part of the database says, "I need to read Page 5," the Pager is responsible for getting it.

**2. The Page Cache: The Librarian's Desk**
The Pager will maintain an in-memory **cache** (for the MVP, a simple HashMap will do). This is like the librarian's desk where recently used books (pages) are kept.
*   When a page is requested, the Pager first checks the cache. If it's there (a "cache hit"), it's returned instantly.
*   If it's not there (a "cache miss"), the Pager reads the page from the disk file, places a copy in the cache, and then returns it.
*   When a page is modified, it's marked as "dirty" in the cache. A `flush` command will later write all dirty pages from the cache back to the disk, persisting the changes.

This caching mechanism is the single most important performance component of any database.

---

### **Phase 3: The MVP - A Usable Database API**

Now we assemble the pieces into a usable API. This is what a developer (initially, you) will interact with.

**The `GraphDB` Object:** This will be the main entry point. Creating one will open the database file and initialize the Pager.

**MVP Functionality:**
*   `add_node()`: This function will orchestrate creating a node. It gets a new ID from the header, serializes the `Node` struct into bytes, asks the Pager for a page with enough free space, and writes the bytes into that page.
*   `add_edge()`: This is the most complex MVP operation, demonstrating the core graph mechanic. It involves:
    1.  Creating and writing the new `Edge` record to a page.
    2.  Finding the source node's record on its page.
    3.  Reading and deserializing the source node.
    4.  Updating its `first_outgoing_edge_id` to point to our new edge (and linking our new edge's `next_` pointer to whatever was there before).
    5.  Re-serializing the source node and writing it back in place.
    6.  Repeating the process for the target node and its `first_incoming_edge_id`.
*   `get_node()`: A simple lookup to retrieve a node by its ID.
*   `get_neighbors()`: The payoff. This function uses the on-disk linked lists. It starts with a node, follows its `first_outgoing_edge_id`, reads that edge, gets the target node, then follows the edge's `next_outgoing_edge_id` to the next neighbor, and so on.

**The MVP is "done" when you can create a database, add nodes and edges, close it, reopen it, and successfully traverse the relationships you created.** It will not be fast for large graphs, nor will it be crash-safe, but it will be *correct*.

---

### **The Roadmap: Future Goals Beyond the MVP**

This is how we evolve our simple MVP into a powerful, production-worthy tool.

**1. Goal: Robustness and Safety**
*   **Transactions (ACID):** Implement a **Write-Ahead Log (WAL)**. This is a journal where we write down what we intend to do *before* we modify the main database file. If the program crashes mid-operation, we can use this journal upon restart to restore the database to a consistent state. This gives us Atomicity and Durability.
*   **Deletion and Space Reclamation:** Implement a system to handle deleting records. When a node is deleted, the space it occupied must be marked as free so it can be reused later. This is often done with a "Free List" or a "Bitmap Page" that tracks usable space.

**2. Goal: High Performance**
*   **Persistent Indexes:** The linked-list traversal is good but not enough. To quickly find the starting node for a query (e.g., "find user with email '...`"), we need indexes. We will implement an on-disk **B-Tree**, a specialized data structure perfect for database indexing.
    *   **Primary Index:** A B-Tree that maps a `NodeId` to its physical location (Page ID and offset).
    *   **Secondary Indexes:** B-Trees that map a property value (like an email address) to the Node IDs that have that value.
*   **Smarter Caching:** Upgrade the simple HashMap cache to a proper **LRU (Least Recently Used)** cache, which will do a much better job of keeping the most relevant pages in memory.

**3. Goal: Concurrency**
*   **Multi-Reader, Single-Writer:** Use Rust's `RwLock` to allow multiple threads to read from the database at the same time, while ensuring that only one thread can write at a time. This is a huge and relatively easy win for many applications.
*   **Finer-Grained Locking:** For expert-level performance, move from a single global lock to a system where we can lock individual pages or data structures, allowing for much higher write throughput.

**4. Goal: Advanced Usability**
*   **A Fluent Query API:** Build a Rust-native, chainable API that makes complex traversals feel natural (e.g., `db.node(user_id).traverse(EdgeType::Follows).nodes()`).
*   **A Declarative Query Language:** The ultimate goal. Design and implement a parser for a simple, Cypher-like query language (`MATCH (a)-[:KNOWS]->(b) RETURN b`). This involves writing a lexer, a parser, a query planner, and an execution engine that translates the query into a series of optimized traversal, filter, and index-lookup operations.

---

### Implementation Task List

- [x] Capture detailed requirements for nodes, edges, and property storage, including serialization formats and validation rules (`docs/data_model.md`).
- [x] Document the page layout, record headers, and allocation strategy for node and edge records within pages.
- [x] Build the pager module with page caching, dirty tracking, and flush mechanics.
- [x] Implement serializers and deserializers for `Node` and `Edge`, ensuring compatibility with the page record format.
- [x] Integrate the pager with an initial `GraphDB` struct that handles file creation, header management, and ID allocation.
- [x] Implement MVP graph mutation APIs (`add_node`, `add_edge`) with correct pointer wiring for on-disk adjacency lists.
- [x] Implement MVP graph read APIs (`get_node`, `get_neighbors`) that leverage cached pages and follow edge chains.
- [x] Add smoke tests or an example program demonstrating open/close cycles, node/edge creation, and traversal.
- [x] Outline follow-up milestones for WAL, free space management, indexing, and concurrency in project tracking tooling (`docs/roadmap.md`).
- [x] Establish coding standards, error handling strategy, and module boundaries to support future contributions (`docs/contributing.md`).

### Immediate Milestones (Completed)

1. Captured node and edge deletion semantics, including free-page recycling, in the `GraphDB::delete_*` flows and regression tests (`tests/smoke.rs`, `tests/stress.rs`).
2. Added higher-level smoke and integration tests that reopen databases and traverse neighbor chains spanning multiple edges (`tests/smoke.rs`).

### Upcoming Milestones: Compaction, WAL, Traversal

**Segmented Page Compaction**
- Design a compactor that walks record pages, copying live records into fresh pages and returning empty pages to the free list.
- Record fragmentation metrics (live bytes vs. capacity) to trigger compaction heuristics and guide future tuning.
- Add tests that simulate heavy churn workloads and assert pointer integrity after compaction cycles.

**Crash Safety via WAL**
- ~~Specify WAL record format (page images vs. logical ops) and integrate it with pager dirty tracking.~~ (see `src/pager/wal.rs`)
- ~~Implement WAL lifecycle: begin transaction, append records, fsync, checkpoint/merge back to main file.~~
- ~~Extend integration tests to power-cycle mid-write, replay the WAL, and verify database invariants.~~

**Richer Traversal APIs**
- Draft a traversal builder that supports filtering by labels/properties and configurable edge directions.
- Provide batched neighbor iteration to stream large result sets without materializing all node IDs at once.
- Create example programs and benchmarks that showcase multi-hop traversals, property filters, and mixed read/write sessions.

**Transactional Commit Layer**
- [x] Introduce transaction begin/commit/rollback scaffolding and track per-transaction dirty pages ahead of WAL commit markers (`src/db.rs`).
- [x] Update pager and GraphDB APIs to route mutations through transactional handles while keeping rollback cheap.
- [x] Add crash and rollback regression tests that cover mixed committed/aborted transactions across restarts.
- [x] Complete comprehensive documentation with user guide and technical reference (`docs/transactional_commit_layer.md`).
