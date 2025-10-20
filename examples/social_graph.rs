//! Social Network Example
//!
//! This example demonstrates building a social network graph with Sombra,
//! including user profiles and friendships.

use chrono::{DateTime, Utc};
use sombra::{GraphDB, Node, Edge, GraphError, NodeId, EdgeId, PropertyValue};
use std::collections::BTreeMap;

struct UserProfile {
    username: String,
    name: String,
    email: String,
    age: u32,
    bio: Option<String>,
    location: Option<String>,
    joined_at: DateTime<Utc>,
    last_active: DateTime<Utc>,
}

struct Post {
    author_id: NodeId,
    content: String,
    created_at: DateTime<Utc>,
    likes: u32,
    shares: u32,
}

struct Comment {
    post_id: NodeId,
    author_id: NodeId,
    content: String,
    created_at: DateTime<Utc>,
}

struct SocialNetwork {
    db: GraphDB,
}

impl SocialNetwork {
    fn new(db_path: &str) -> Result<Self, GraphError> {
        let db = GraphDB::open(db_path)?;
        Ok(SocialNetwork { db })
    }

    /// Create a new user
    fn create_user(&mut self, profile: UserProfile) -> Result<NodeId, GraphError> {
        let mut tx = self.db.begin_transaction()?;

        let mut properties = BTreeMap::new();
        properties.insert("username".to_string(), PropertyValue::String(profile.username));
        properties.insert("name".to_string(), PropertyValue::String(profile.name));
        properties.insert("email".to_string(), PropertyValue::String(profile.email));
        properties.insert("age".to_string(), PropertyValue::Int(profile.age as i64));
        if let Some(bio) = profile.bio {
            properties.insert("bio".to_string(), PropertyValue::String(bio));
        }
        if let Some(location) = profile.location {
            properties.insert("location".to_string(), PropertyValue::String(location));
        }
        properties.insert("joined_at".to_string(), PropertyValue::Int(profile.joined_at.timestamp()));
        properties.insert("last_active".to_string(), PropertyValue::Int(profile.last_active.timestamp()));

        let mut node = Node::new(0);
        node.properties = properties;
        node.labels.push("User".to_string());

        let node_id = tx.add_node(node)?;
        tx.commit()?;
        Ok(node_id)
    }

    /// Create a friendship between two users
    fn create_friendship(
        &mut self,
        user1_id: NodeId,
        user2_id: NodeId,
        since: DateTime<Utc>,
    ) -> Result<EdgeId, GraphError> {
        let mut tx = self.db.begin_transaction()?;
        let mut edge = Edge::new(0, user1_id, user2_id, "FRIENDS_WITH");
        edge.properties.insert("since".to_string(), PropertyValue::Int(since.timestamp()));
        let edge_id = tx.add_edge(edge)?;
        tx.commit()?;
        Ok(edge_id)
    }

    /// Get user's friends
    fn get_user_friends(&mut self, user_id: NodeId) -> Result<Vec<NodeId>, GraphError> {
        let mut tx = self.db.begin_transaction()?;
        let neighbors = tx.get_neighbors(user_id)?;
        tx.commit()?;
        Ok(neighbors)
    }

    /// Get user's posts
    fn get_user_posts(&mut self, user_id: NodeId) -> Result<Vec<NodeId>, GraphError> {
        let mut tx = self.db.begin_transaction()?;
        let post_ids = tx.get_nodes_by_label("Post")?;
        let mut user_posts = Vec::new();
        for post_id in post_ids {
            let post_node = tx.get_node(post_id)?;
            if let Some(edge_id) = post_node.first_incoming_edge_id.checked_sub(0) {
                let edge = tx.get_edge(edge_id)?;
                if edge.source_node_id == user_id {
                    user_posts.push(post_id);
                }
            }
        }
        tx.commit()?;
        Ok(user_posts)
    }

    /// Get comments on a post
    fn get_post_comments(&mut self, post_id: NodeId) -> Result<Vec<NodeId>, GraphError> {
        let mut tx = self.db.begin_transaction()?;
        let neighbor_ids = tx.get_neighbors(post_id)?;
        let mut comments = Vec::new();
        for neighbor_id in neighbor_ids {
            let neighbor_node = tx.get_node(neighbor_id)?;
            if neighbor_node.labels.contains(&"Comment".to_string()) {
                comments.push(neighbor_id);
            }
        }
        tx.commit()?;
        Ok(comments)
    }

    fn create_post(&mut self, post: Post) -> Result<NodeId, GraphError> {
        let mut tx = self.db.begin_transaction()?;

        let mut properties = BTreeMap::new();
        properties.insert("content".to_string(), PropertyValue::String(post.content));
        properties.insert("created_at".to_string(), PropertyValue::Int(post.created_at.timestamp()));
        properties.insert("likes".to_string(), PropertyValue::Int(post.likes as i64));
        properties.insert("shares".to_string(), PropertyValue::Int(post.shares as i64));

        let mut node = Node::new(0);
        node.properties = properties;
        node.labels.push("Post".to_string());

        let post_id = tx.add_node(node)?;

        let edge = Edge::new(0, post.author_id, post_id, "POSTED");
        tx.add_edge(edge)?;

        tx.commit()?;
        Ok(post_id)
    }

    fn create_comment(&mut self, comment: Comment) -> Result<NodeId, GraphError> {
        let mut tx = self.db.begin_transaction()?;

        let mut properties = BTreeMap::new();
        properties.insert("content".to_string(), PropertyValue::String(comment.content));
        properties.insert("created_at".to_string(), PropertyValue::Int(comment.created_at.timestamp()));

        let mut node = Node::new(0);
        node.properties = properties;
        node.labels.push("Comment".to_string());

        let comment_id = tx.add_node(node)?;

        let edge = Edge::new(0, comment.author_id, comment_id, "COMMENTED");
        tx.add_edge(edge)?;

        let edge = Edge::new(0, comment_id, comment.post_id, "ON_POST");
        tx.add_edge(edge)?;

        tx.commit()?;
        Ok(comment_id)
    }
}

fn main() -> Result<(), GraphError> {
    println!("🌐 Social Network Example with Sombra");

    // Initialize social network
    let mut social_network = SocialNetwork::new("social_network.db")?;

    // Create sample users
    let now = Utc::now();

    let alice_id = social_network.create_user(UserProfile {
        username: "alice_smith".to_string(),
        name: "Alice Smith".to_string(),
        email: "alice@example.com".to_string(),
        age: 28,
        bio: Some("Software engineer who loves hiking and photography".to_string()),
        location: Some("San Francisco, CA".to_string()),
        joined_at: now - chrono::Duration::days(365),
        last_active: now,
    })?;

    let bob_id = social_network.create_user(UserProfile {
        username: "bob_jones".to_string(),
        name: "Bob Jones".to_string(),
        email: "bob@example.com".to_string(),
        age: 32,
        bio: Some("Data scientist and coffee enthusiast".to_string()),
        location: Some("Seattle, WA".to_string()),
        joined_at: now - chrono::Duration::days(200),
        last_active: now,
    })?;

    println!("✅ Created 2 users");

    // Create friendships
    social_network.create_friendship(alice_id, bob_id, now - chrono::Duration::days(180))?;

    println!("✅ Created friendships");

    // Create posts
    let post1_id = social_network.create_post(Post {
        author_id: alice_id,
        content: "Just finished a amazing hike in the mountains! 🏔️ The view was breathtaking.".to_string(),
        created_at: now - chrono::Duration::hours(2),
        likes: 5,
        shares: 2,
    })?;

    let post2_id = social_network.create_post(Post {
        author_id: bob_id,
        content: "Excited to share that I'll be speaking at the Data Science Conference next month! 🎤".to_string(),
        created_at: now - chrono::Duration::hours(4),
        likes: 12,
        shares: 8,
    })?;

    println!("✅ Created posts");

    // Create comments
    let _comment1_id = social_network.create_comment(Comment {
        post_id: post1_id,
        author_id: bob_id,
        content: "Looks incredible! Which trail did you take?".to_string(),
        created_at: now - chrono::Duration::hours(1),
    })?;

    println!("✅ Created comments");

    // Demonstrate social network features
    println!("\n📊 Social Network Analysis:");

    // Alice's friends
    let alice_friends = social_network.get_user_friends(alice_id)?;
    println!("Alice's friends: {}", alice_friends.len());

    // Alice's posts
    let alice_posts = social_network.get_user_posts(alice_id)?;
    println!("Alice's posts: {}", alice_posts.len());

    // Comments on Bob's post
    let post2_comments = social_network.get_post_comments(post2_id)?;
    println!(
        "Comments on Bob's conference post: {}",
        post2_comments.len()
    );

    println!("
🎉 Social network example completed successfully!");
    println!("Database saved to: social_network.db");

    Ok(())
}