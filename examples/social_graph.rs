//! Social Network Example
//!
//! This example demonstrates building a social network graph with Sombra,
//! including user profiles, friendships, content creation, and social interactions.

use chrono::{DateTime, Utc};
use sombra::{Edge, GraphDB, Node, PropertyValue, Result};
use std::collections::HashMap;

#[derive(Debug, Clone)]
struct UserProfile {
    id: Option<u64>,
    username: String,
    name: String,
    email: String,
    age: u32,
    bio: Option<String>,
    location: Option<String>,
    joined_at: DateTime<Utc>,
    last_active: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct Post {
    id: Option<u64>,
    author_id: u64,
    content: String,
    created_at: DateTime<Utc>,
    likes: u32,
    shares: u32,
}

#[derive(Debug, Clone)]
struct Comment {
    id: Option<u64>,
    post_id: u64,
    author_id: u64,
    content: String,
    created_at: DateTime<Utc>,
}

struct SocialNetwork {
    db: GraphDB,
}

impl SocialNetwork {
    fn new(db_path: &str) -> Result<Self> {
        let db = GraphDB::open(db_path)?;
        Ok(SocialNetwork { db })
    }

    /// Create a new user profile
    fn create_user(&mut self, profile: UserProfile) -> Result<Node> {
        let mut tx = self.db.begin_transaction()?;

        let mut properties = vec![
            ("username".into(), PropertyValue::String(profile.username)),
            ("name".into(), PropertyValue::String(profile.name)),
            ("email".into(), PropertyValue::String(profile.email)),
            ("age".into(), PropertyValue::Integer(profile.age as i64)),
            (
                "joined_at".into(),
                PropertyValue::Integer(profile.joined_at.timestamp()),
            ),
            (
                "last_active".into(),
                PropertyValue::Integer(profile.last_active.timestamp()),
            ),
        ];

        if let Some(bio) = profile.bio {
            properties.push(("bio".into(), PropertyValue::String(bio)));
        }

        if let Some(location) = profile.location {
            properties.push(("location".into(), PropertyValue::String(location)));
        }

        let user_node = tx.create_node("User", properties)?;
        tx.commit()?;
        Ok(user_node)
    }

    /// Create a friendship between two users
    fn create_friendship(
        &mut self,
        user1_id: u64,
        user2_id: u64,
        since: DateTime<Utc>,
    ) -> Result<Edge> {
        let mut tx = self.db.begin_transaction()?;

        let edge = tx.create_edge(
            user1_id,
            user2_id,
            "FRIENDS_WITH",
            vec![
                ("since".into(), PropertyValue::Integer(since.timestamp())),
                ("status".into(), PropertyValue::String("active".to_string())),
            ],
        )?;

        tx.commit()?;
        Ok(edge)
    }

    /// Create a post
    fn create_post(&mut self, post: Post) -> Result<Node> {
        let mut tx = self.db.begin_transaction()?;

        let post_node = tx.create_node(
            "Post",
            vec![
                ("content".into(), PropertyValue::String(post.content)),
                (
                    "created_at".into(),
                    PropertyValue::Integer(post.created_at.timestamp()),
                ),
                ("likes".into(), PropertyValue::Integer(post.likes as i64)),
                ("shares".into(), PropertyValue::Integer(post.shares as i64)),
            ],
        )?;

        // Link post to author
        tx.create_edge(
            post.author_id,
            post_node.id,
            "AUTHORED",
            vec![(
                "created_at".into(),
                PropertyValue::Integer(post.created_at.timestamp()),
            )],
        )?;

        tx.commit()?;
        Ok(post_node)
    }

    /// Create a comment on a post
    fn create_comment(&mut self, comment: Comment) -> Result<Node> {
        let mut tx = self.db.begin_transaction()?;

        let comment_node = tx.create_node(
            "Comment",
            vec![
                ("content".into(), PropertyValue::String(comment.content)),
                (
                    "created_at".into(),
                    PropertyValue::Integer(comment.created_at.timestamp()),
                ),
            ],
        )?;

        // Link comment to post
        tx.create_edge(
            comment.post_id,
            comment_node.id,
            "HAS_COMMENT",
            vec![(
                "created_at".into(),
                PropertyValue::Integer(comment.created_at.timestamp()),
            )],
        )?;

        // Link comment to author
        tx.create_edge(
            comment.author_id,
            comment_node.id,
            "AUTHORED",
            vec![(
                "created_at".into(),
                PropertyValue::Integer(comment.created_at.timestamp()),
            )],
        )?;

        tx.commit()?;
        Ok(comment_node)
    }

    /// Like a post
    fn like_post(&mut self, user_id: u64, post_id: u64, liked_at: DateTime<Utc>) -> Result<Edge> {
        let mut tx = self.db.begin_transaction()?;

        // Create like relationship
        tx.create_edge(
            user_id,
            post_id,
            "LIKES",
            vec![(
                "liked_at".into(),
                PropertyValue::Integer(liked_at.timestamp()),
            )],
        )?;

        // Increment like count on post
        let post_props = tx.get_node_properties(post_id)?;
        let current_likes = post_props
            .get("likes")
            .and_then(|v| v.as_integer())
            .unwrap_or(0);

        tx.update_node_properties(
            post_id,
            vec![("likes".into(), PropertyValue::Integer(current_likes + 1))],
        )?;

        tx.commit()?;
        Ok(tx.get_edge(1)?) // This is a placeholder - in real implementation you'd get the edge ID
    }

    /// Share a post
    fn share_post(&mut self, user_id: u64, post_id: u64, shared_at: DateTime<Utc>) -> Result<Edge> {
        let mut tx = self.db.begin_transaction()?;

        // Create share relationship
        tx.create_edge(
            user_id,
            post_id,
            "SHARED",
            vec![(
                "shared_at".into(),
                PropertyValue::Integer(shared_at.timestamp()),
            )],
        )?;

        // Increment share count on post
        let post_props = tx.get_node_properties(post_id)?;
        let current_shares = post_props
            .get("shares")
            .and_then(|v| v.as_integer())
            .unwrap_or(0);

        tx.update_node_properties(
            post_id,
            vec![("shares".into(), PropertyValue::Integer(current_shares + 1))],
        )?;

        tx.commit()?;
        Ok(tx.get_edge(1)?) // Placeholder
    }

    /// Get user's friends
    fn get_user_friends(&self, user_id: u64) -> Result<Vec<Node>> {
        let tx = self.db.begin_transaction()?;

        let friends = tx
            .traverse()
            .from_node(user_id)
            .outgoing("FRIENDS_WITH")
            .collect::<Result<Vec<_>>>()?;

        Ok(friends)
    }

    /// Get user's posts
    fn get_user_posts(&self, user_id: u64) -> Result<Vec<Node>> {
        let tx = self.db.begin_transaction()?;

        let posts = tx
            .traverse()
            .from_node(user_id)
            .outgoing("AUTHORED")
            .filter(|node| {
                // Filter for Post nodes
                let tx_inner = self.db.begin_transaction().unwrap();
                let props = tx_inner.get_node_properties(node.id).unwrap();
                Ok(node.label == "Post")
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(posts)
    }

    /// Get comments on a post
    fn get_post_comments(&self, post_id: u64) -> Result<Vec<Node>> {
        let tx = self.db.begin_transaction()?;

        let comments = tx
            .traverse()
            .from_node(post_id)
            .outgoing("HAS_COMMENT")
            .collect::<Result<Vec<_>>>()?;

        Ok(comments)
    }

    /// Get friends of friends (2-hop network)
    fn get_friends_of_friends(&self, user_id: u64) -> Result<Vec<Node>> {
        let tx = self.db.begin_transaction()?;

        let fofs = tx
            .traverse()
            .from_node(user_id)
            .outgoing("FRIENDS_WITH")
            .outgoing("FRIENDS_WITH")
            .collect::<Result<Vec<_>>>()?;

        Ok(fofs)
    }

    /// Get mutual friends between two users
    fn get_mutual_friends(&self, user1_id: u64, user2_id: u64) -> Result<Vec<Node>> {
        let tx = self.db.begin_transaction()?;

        // Get user1's friends
        let user1_friends = tx
            .traverse()
            .from_node(user1_id)
            .outgoing("FRIENDS_WITH")
            .collect::<Result<Vec<_>>>()?;

        // Get user2's friends
        let user2_friends = tx
            .traverse()
            .from_node(user2_id)
            .outgoing("FRIENDS_WITH")
            .collect::<Result<Vec<_>>>()?;

        // Find intersection
        let user1_friend_ids: std::collections::HashSet<u64> =
            user1_friends.iter().map(|f| f.id).collect();
        let user2_friend_ids: std::collections::HashSet<u64> =
            user2_friends.iter().map(|f| f.id).collect();

        let mutual_ids: Vec<u64> = user1_friend_ids
            .intersection(&user2_friend_ids)
            .cloned()
            .collect();

        // Get full node objects for mutual friends
        let mut mutual_friends = Vec::new();
        for friend_id in mutual_ids {
            let friend = tx.get_node(friend_id)?;
            mutual_friends.push(friend);
        }

        Ok(mutual_friends)
    }

    /// Get user's network statistics
    fn get_user_network_stats(&self, user_id: u64) -> Result<NetworkStats> {
        let tx = self.db.begin_transaction()?;

        // Get direct friends
        let friends = tx
            .traverse()
            .from_node(user_id)
            .outgoing("FRIENDS_WITH")
            .collect::<Result<Vec<_>>>()?;

        // Get friends of friends
        let fofs = tx
            .traverse()
            .from_node(user_id)
            .outgoing("FRIENDS_WITH")
            .outgoing("FRIENDS_WITH")
            .collect::<Result<Vec<_>>>()?;

        // Get user's posts
        let posts = tx
            .traverse()
            .from_node(user_id)
            .outgoing("AUTHORED")
            .filter(|node| Ok(node.label == "Post"))
            .collect::<Result<Vec<_>>>()?;

        // Count total likes on user's posts
        let mut total_likes = 0;
        let mut total_shares = 0;

        for post in &posts {
            let props = tx.get_node_properties(post.id)?;
            total_likes += props.get("likes").and_then(|v| v.as_integer()).unwrap_or(0);
            total_shares += props
                .get("shares")
                .and_then(|v| v.as_integer())
                .unwrap_or(0);
        }

        Ok(NetworkStats {
            friends_count: friends.len(),
            friends_of_friends_count: fofs.len(),
            posts_count: posts.len(),
            total_likes,
            total_shares,
        })
    }

    /// Find people you may know (friends of friends not already friends)
    fn find_people_you_may_know(&self, user_id: u64, limit: usize) -> Result<Vec<Node>> {
        let tx = self.db.begin_transaction()?;

        // Get current friends
        let current_friends = tx
            .traverse()
            .from_node(user_id)
            .outgoing("FRIENDS_WITH")
            .collect::<Result<Vec<_>>>()?;

        let current_friend_ids: std::collections::HashSet<u64> =
            current_friends.iter().map(|f| f.id).collect();

        // Get friends of friends
        let fofs = tx
            .traverse()
            .from_node(user_id)
            .outgoing("FRIENDS_WITH")
            .outgoing("FRIENDS_WITH")
            .collect::<Result<Vec<_>>>()?;

        // Filter out current friends and the user themselves
        let mut suggestions = Vec::new();
        for fof in fofs {
            if fof.id != user_id && !current_friend_ids.contains(&fof.id) {
                suggestions.push(fof);
            }
        }

        // Sort by number of mutual friends (simplified - would need more complex query)
        suggestions.truncate(limit);

        Ok(suggestions)
    }
}

#[derive(Debug)]
struct NetworkStats {
    friends_count: usize,
    friends_of_friends_count: usize,
    posts_count: usize,
    total_likes: i64,
    total_shares: i64,
}

fn main() -> Result<()> {
    println!("🌐 Social Network Example with Sombra");

    // Initialize social network
    let mut social_network = SocialNetwork::new("social_network.db")?;

    // Create sample users
    let now = Utc::now();

    let alice = social_network.create_user(UserProfile {
        id: None,
        username: "alice_smith".to_string(),
        name: "Alice Smith".to_string(),
        email: "alice@example.com".to_string(),
        age: 28,
        bio: Some("Software engineer who loves hiking and photography".to_string()),
        location: Some("San Francisco, CA".to_string()),
        joined_at: now - chrono::Duration::days(365),
        last_active: now,
    })?;

    let bob = social_network.create_user(UserProfile {
        id: None,
        username: "bob_jones".to_string(),
        name: "Bob Jones".to_string(),
        email: "bob@example.com".to_string(),
        age: 32,
        bio: Some("Data scientist and coffee enthusiast".to_string()),
        location: Some("Seattle, WA".to_string()),
        joined_at: now - chrono::Duration::days(200),
        last_active: now,
    })?;

    let charlie = social_network.create_user(UserProfile {
        id: None,
        username: "charlie_brown".to_string(),
        name: "Charlie Brown".to_string(),
        email: "charlie@example.com".to_string(),
        age: 25,
        bio: Some("Graduate student in machine learning".to_string()),
        location: Some("Boston, MA".to_string()),
        joined_at: now - chrono::Duration::days(150),
        last_active: now,
    })?;

    let diana = social_network.create_user(UserProfile {
        id: None,
        username: "diana_prince".to_string(),
        name: "Diana Prince".to_string(),
        email: "diana@example.com".to_string(),
        age: 30,
        bio: Some("Product manager and yoga instructor".to_string()),
        location: Some("New York, NY".to_string()),
        joined_at: now - chrono::Duration::days(100),
        last_active: now,
    })?;

    println!("✅ Created 4 users");

    // Create friendships
    social_network.create_friendship(alice.id, bob.id, now - chrono::Duration::days(180))?;
    social_network.create_friendship(alice.id, charlie.id, now - chrono::Duration::days(120))?;
    social_network.create_friendship(bob.id, charlie.id, now - chrono::Duration::days(90))?;
    social_network.create_friendship(charlie.id, diana.id, now - chrono::Duration::days(60))?;

    println!("✅ Created friendships");

    // Create posts
    let post1 = social_network.create_post(Post {
        id: None,
        author_id: alice.id,
        content: "Just finished a amazing hike in the mountains! 🏔️ The view was breathtaking."
            .to_string(),
        created_at: now - chrono::Duration::hours(2),
        likes: 5,
        shares: 2,
    })?;

    let post2 = social_network.create_post(Post {
        id: None,
        author_id: bob.id,
        content:
            "Excited to share that I'll be speaking at the Data Science Conference next month! 🎤"
                .to_string(),
        created_at: now - chrono::Duration::hours(4),
        likes: 12,
        shares: 8,
    })?;

    let post3 = social_network.create_post(Post {
        id: None,
        author_id: charlie.id,
        content: "Working on an interesting ML project involving natural language processing. Any recommendations for good datasets?" .to_string(),
        created_at: now - chrono::Duration::hours(6),
        likes: 3,
        shares: 1,
    })?;

    println!("✅ Created posts");

    // Create comments
    let comment1 = social_network.create_comment(Comment {
        id: None,
        post_id: post1.id,
        author_id: bob.id,
        content: "Looks incredible! Which trail did you take?".to_string(),
        created_at: now - chrono::Duration::hours(1),
    })?;

    let comment2 = social_network.create_comment(Comment {
        id: None,
        post_id: post2.id,
        author_id: alice.id,
        content: "Congratulations Bob! That's fantastic news! 🎉".to_string(),
        created_at: now - chrono::Duration::hours(3),
    })?;

    let comment3 = social_network.create_comment(Comment {
        id: None,
        post_id: post3.id,
        author_id: diana.id,
        content: "Have you tried the GLUE benchmark? It has some great NLP datasets.".to_string(),
        created_at: now - chrono::Duration::hours(5),
    })?;

    println!("✅ Created comments");

    // Add likes and shares
    social_network.like_post(bob.id, post1.id, now - chrono::Duration::minutes(90))?;
    social_network.like_post(charlie.id, post1.id, now - chrono::Duration::minutes(60))?;
    social_network.like_post(alice.id, post2.id, now - chrono::Duration::minutes(120))?;
    social_network.like_post(charlie.id, post2.id, now - chrono::Duration::minutes(100))?;
    social_network.like_post(diana.id, post2.id, now - chrono::Duration::minutes(80))?;

    social_network.share_post(alice.id, post2.id, now - chrono::Duration::minutes(110))?;
    social_network.share_post(charlie.id, post2.id, now - chrono::Duration::minutes(90))?;

    println!("✅ Added likes and shares");

    // Demonstrate social network features
    println!("\n📊 Social Network Analysis:");

    // Alice's friends
    let alice_friends = social_network.get_user_friends(alice.id)?;
    println!("Alice's friends: {}", alice_friends.len());

    // Alice's posts
    let alice_posts = social_network.get_user_posts(alice.id)?;
    println!("Alice's posts: {}", alice_posts.len());

    // Comments on Bob's post
    let post2_comments = social_network.get_post_comments(post2.id)?;
    println!(
        "Comments on Bob's conference post: {}",
        post2_comments.len()
    );

    // Friends of friends
    let alice_fofs = social_network.get_friends_of_friends(alice.id)?;
    println!("Alice's friends of friends: {}", alice_fofs.len());

    // Mutual friends between Alice and Charlie
    let mutual_friends = social_network.get_mutual_friends(alice.id, charlie.id)?;
    println!(
        "Mutual friends between Alice and Charlie: {}",
        mutual_friends.len()
    );

    // Network statistics for each user
    println!("\n📈 Network Statistics:");
    for (user_id, name) in [
        (alice.id, "Alice"),
        (bob.id, "Bob"),
        (charlie.id, "Charlie"),
        (diana.id, "Diana"),
    ] {
        let stats = social_network.get_user_network_stats(user_id)?;
        println!(
            "{}: {} friends, {} friends-of-friends, {} posts, {} total likes, {} total shares",
            name,
            stats.friends_count,
            stats.friends_of_friends_count,
            stats.posts_count,
            stats.total_likes,
            stats.total_shares
        );
    }

    // People you may know for Alice
    let suggestions = social_network.find_people_you_may_know(alice.id, 5)?;
    println!("\n💡 People Alice may know:");
    for suggestion in suggestions {
        let tx = social_network.db.begin_transaction()?;
        let props = tx.get_node_properties(suggestion.id)?;
        let name = props
            .get("name")
            .and_then(|v| v.as_string())
            .unwrap_or("Unknown");
        println!("  - {}", name);
    }

    println!("\n🎉 Social network example completed successfully!");
    println!("Database saved to: social_network.db");

    Ok(())
}
