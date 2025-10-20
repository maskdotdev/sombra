//! Recommendation Engine Example
//!
//! This example demonstrates building a recommendation engine with Sombra,
//! including collaborative filtering, content-based filtering, and hybrid approaches.

use serde::{Deserialize, Serialize};
use sombra::{Edge, GraphDB, Node, PropertyValue, Result};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct User {
    id: Option<u64>,
    username: String,
    name: String,
    age: u32,
    interests: Vec<String>,
    location: Option<String>,
    join_date: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Item {
    id: Option<u64>,
    title: String,
    category: String,
    tags: Vec<String>,
    description: Option<String>,
    rating: f64,
    num_ratings: u32,
    created_date: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Rating {
    id: Option<u64>,
    user_id: u64,
    item_id: u64,
    rating: f64,
    timestamp: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone)]
struct Recommendation {
    item_id: u64,
    title: String,
    score: f64,
    reason: String,
}

struct RecommendationEngine {
    db: GraphDB,
}

impl RecommendationEngine {
    fn new(db_path: &str) -> Result<Self> {
        let db = GraphDB::open(db_path)?;
        Ok(RecommendationEngine { db })
    }

    /// Add a user to the system
    fn add_user(&mut self, user: User) -> Result<Node> {
        let mut tx = self.db.begin_transaction()?;

        let mut properties = vec![
            (
                "username".into(),
                PropertyValue::String(user.username.clone()),
            ),
            ("name".into(), PropertyValue::String(user.name.clone())),
            ("age".into(), PropertyValue::Integer(user.age as i64)),
            (
                "join_date".into(),
                PropertyValue::Integer(user.join_date.timestamp()),
            ),
        ];

        if let Some(location) = user.location {
            properties.push(("location".into(), PropertyValue::String(location)));
        }

        // Add interests as comma-separated string
        if !user.interests.is_empty() {
            properties.push((
                "interests".into(),
                PropertyValue::String(user.interests.join(", ")),
            ));
        }

        let user_node = tx.create_node("User", properties)?;
        tx.commit()?;
        Ok(user_node)
    }

    /// Add an item to the system
    fn add_item(&mut self, item: Item) -> Result<Node> {
        let mut tx = self.db.begin_transaction()?;

        let mut properties = vec![
            ("title".into(), PropertyValue::String(item.title.clone())),
            (
                "category".into(),
                PropertyValue::String(item.category.clone()),
            ),
            ("rating".into(), PropertyValue::Float(item.rating)),
            (
                "num_ratings".into(),
                PropertyValue::Integer(item.num_ratings as i64),
            ),
            (
                "created_date".into(),
                PropertyValue::Integer(item.created_date.timestamp()),
            ),
        ];

        if let Some(description) = item.description {
            properties.push(("description".into(), PropertyValue::String(description)));
        }

        // Add tags as comma-separated string
        if !item.tags.is_empty() {
            properties.push(("tags".into(), PropertyValue::String(item.tags.join(", "))));
        }

        let item_node = tx.create_node("Item", properties)?;
        tx.commit()?;
        Ok(item_node)
    }

    /// Add a rating from a user to an item
    fn add_rating(&mut self, rating: Rating) -> Result<Edge> {
        let mut tx = self.db.begin_transaction()?;

        let edge = tx.create_edge(
            rating.user_id,
            rating.item_id,
            "RATED",
            vec![
                ("rating".into(), PropertyValue::Float(rating.rating)),
                (
                    "timestamp".into(),
                    PropertyValue::Integer(rating.timestamp.timestamp()),
                ),
            ],
        )?;

        // Update item's average rating
        let item_props = tx.get_node_properties(rating.item_id)?;
        let current_rating = item_props
            .get("rating")
            .and_then(|v| v.as_float())
            .unwrap_or(0.0);
        let current_num_ratings = item_props
            .get("num_ratings")
            .and_then(|v| v.as_integer())
            .unwrap_or(0);

        let new_num_ratings = current_num_ratings + 1;
        let new_rating =
            (current_rating * current_num_ratings as f64 + rating.rating) / new_num_ratings as f64;

        tx.update_node_properties(
            rating.item_id,
            vec![
                ("rating".into(), PropertyValue::Float(new_rating)),
                (
                    "num_ratings".into(),
                    PropertyValue::Integer(new_num_ratings),
                ),
            ],
        )?;

        tx.commit()?;
        Ok(edge)
    }

    /// Get items rated by a user
    fn get_user_ratings(&self, user_id: u64) -> Result<Vec<(u64, f64)>> {
        let tx = self.db.begin_transaction()?;

        let edges = tx.get_outgoing_edges(user_id)?;
        let mut ratings = Vec::new();

        for edge in edges {
            if edge.label == "RATED" {
                let edge_props = tx.get_edge_properties(edge.id)?;
                if let Some(rating) = edge_props.get("rating").and_then(|v| v.as_float()) {
                    ratings.push((edge.to_node, rating));
                }
            }
        }

        Ok(ratings)
    }

    /// Get users who rated an item
    fn get_item_ratings(&self, item_id: u64) -> Result<Vec<(u64, f64)>> {
        let tx = self.db.begin_transaction()?;

        let edges = tx.get_incoming_edges(item_id)?;
        let mut ratings = Vec::new();

        for edge in edges {
            if edge.label == "RATED" {
                let edge_props = tx.get_edge_properties(edge.id)?;
                if let Some(rating) = edge_props.get("rating").and_then(|v| v.as_float()) {
                    ratings.push((edge.from_node, rating));
                }
            }
        }

        Ok(ratings)
    }

    /// Find similar users based on rating patterns (collaborative filtering)
    fn find_similar_users(&self, user_id: u64, min_common_items: usize) -> Result<Vec<(u64, f64)>> {
        let user_ratings = self.get_user_ratings(user_id)?;
        let user_rating_map: HashMap<u64, f64> = user_ratings.into_iter().collect();

        let tx = self.db.begin_transaction()?;
        let users = tx.find_nodes_by_label("User")?;
        let mut similarities = Vec::new();

        for user in users {
            if user.id == user_id {
                continue;
            }

            let other_ratings = self.get_user_ratings(user.id)?;
            let other_rating_map: HashMap<u64, f64> = other_ratings.into_iter().collect();

            // Find common rated items
            let common_items: Vec<u64> = user_rating_map
                .keys()
                .filter(|item_id| other_rating_map.contains_key(item_id))
                .cloned()
                .collect();

            if common_items.len() >= min_common_items {
                // Calculate cosine similarity
                let mut dot_product = 0.0;
                let mut norm_a = 0.0;
                let mut norm_b = 0.0;

                for item_id in &common_items {
                    let rating_a = user_rating_map[item_id];
                    let rating_b = other_rating_map[item_id];

                    dot_product += rating_a * rating_b;
                    norm_a += rating_a * rating_a;
                    norm_b += rating_b * rating_b;
                }

                if norm_a > 0.0 && norm_b > 0.0 {
                    let similarity = dot_product / (norm_a.sqrt() * norm_b.sqrt());
                    similarities.push((user.id, similarity));
                }
            }
        }

        // Sort by similarity (descending)
        similarities.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        Ok(similarities)
    }

    /// Collaborative filtering recommendations
    fn collaborative_filtering_recommendations(
        &self,
        user_id: u64,
        num_recommendations: usize,
    ) -> Result<Vec<Recommendation>> {
        let user_ratings = self.get_user_ratings(user_id)?;
        let user_rated_items: HashSet<u64> = user_ratings.iter().map(|(id, _)| *id).collect();

        // Find similar users
        let similar_users = self.find_similar_users(user_id, 3)?;
        if similar_users.is_empty() {
            return Ok(Vec::new());
        }

        // Get items rated by similar users
        let mut item_scores: HashMap<u64, (f64, usize)> = HashMap::new();

        for (similar_user_id, similarity) in similar_users.iter().take(10) {
            let similar_user_ratings = self.get_user_ratings(*similar_user_id)?;

            for (item_id, rating) in similar_user_ratings {
                if !user_rated_items.contains(&item_id) {
                    let entry = item_scores.entry(item_id).or_insert((0.0, 0));
                    entry.0 += rating * similarity;
                    entry.1 += 1;
                }
            }
        }

        // Calculate average scores and sort
        let mut recommendations: Vec<Recommendation> = item_scores
            .into_iter()
            .filter_map(|(item_id, (score, count))| {
                if count >= 2 {
                    // At least 2 similar users rated it
                    let avg_score = score / count as f64;
                    let tx = self.db.begin_transaction().ok()?;
                    let item_props = tx.get_node_properties(item_id).ok()?;
                    let title = item_props
                        .get("title")
                        .and_then(|v| v.as_string())
                        .map(|s| s.to_string())?;

                    Some(Recommendation {
                        item_id,
                        title,
                        score: avg_score,
                        reason: format!("Users with similar taste rated this {:.1}/5.0", avg_score),
                    })
                } else {
                    None
                }
            })
            .collect();

        recommendations.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        recommendations.truncate(num_recommendations);
        Ok(recommendations)
    }

    /// Content-based filtering recommendations
    fn content_based_recommendations(
        &self,
        user_id: u64,
        num_recommendations: usize,
    ) -> Result<Vec<Recommendation>> {
        let tx = self.db.begin_transaction()?;

        // Get user's interests
        let user_node = tx.get_node(user_id)?;
        let user_props = tx.get_node_properties(user_id)?;
        let interests_str = user_props
            .get("interests")
            .and_then(|v| v.as_string())
            .unwrap_or("");
        let user_interests: HashSet<String> = interests_str
            .split(", ")
            .map(|s| s.trim().to_lowercase())
            .collect();

        // Get user's rated items
        let user_ratings = self.get_user_ratings(user_id)?;
        let user_rated_items: HashSet<u64> = user_ratings.iter().map(|(id, _)| *id).collect();

        // Get all items
        let items = tx.find_nodes_by_label("Item")?;
        let mut recommendations = Vec::new();

        for item in items {
            if user_rated_items.contains(&item.id) {
                continue;
            }

            let item_props = tx.get_node_properties(item.id)?;

            // Calculate content similarity score
            let mut score = 0.0;

            // Category match
            if let Some(category) = item_props.get("category").and_then(|v| v.as_string()) {
                if user_interests.contains(&category.to_lowercase()) {
                    score += 2.0;
                }
            }

            // Tags match
            if let Some(tags_str) = item_props.get("tags").and_then(|v| v.as_string()) {
                let item_tags: HashSet<String> = tags_str
                    .split(", ")
                    .map(|s| s.trim().to_lowercase())
                    .collect();

                for interest in &user_interests {
                    if item_tags.contains(interest) {
                        score += 1.0;
                    }
                }
            }

            // Boost by item rating
            if let Some(rating) = item_props.get("rating").and_then(|v| v.as_float()) {
                score *= rating / 5.0; // Normalize by max rating
            }

            if score > 0.0 {
                let title = item_props
                    .get("title")
                    .and_then(|v| v.as_string())
                    .unwrap_or("Unknown")
                    .to_string();

                recommendations.push(Recommendation {
                    item_id: item.id,
                    title,
                    score,
                    reason: format!("Matches your interests: {}", interests_str),
                });
            }
        }

        recommendations.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        recommendations.truncate(num_recommendations);
        Ok(recommendations)
    }

    /// Hybrid recommendations combining collaborative and content-based
    fn hybrid_recommendations(
        &self,
        user_id: u64,
        num_recommendations: usize,
        collaborative_weight: f64,
    ) -> Result<Vec<Recommendation>> {
        let collaborative_recs =
            self.collaborative_filtering_recommendations(user_id, num_recommendations * 2)?;
        let content_recs = self.content_based_recommendations(user_id, num_recommendations * 2)?;

        let mut combined_scores: HashMap<u64, (f64, String)> = HashMap::new();

        // Add collaborative recommendations
        for rec in collaborative_recs {
            let entry = combined_scores
                .entry(rec.item_id)
                .or_insert((0.0, String::new()));
            entry.0 += rec.score * collaborative_weight;
            if entry.1.is_empty() {
                entry.1 = rec.reason;
            }
        }

        // Add content-based recommendations
        for rec in content_recs {
            let entry = combined_scores
                .entry(rec.item_id)
                .or_insert((0.0, String::new()));
            entry.0 += rec.score * (1.0 - collaborative_weight);
            if entry.1.is_empty() {
                entry.1 = rec.reason;
            }
        }

        // Convert back to recommendations
        let mut recommendations: Vec<Recommendation> = combined_scores
            .into_iter()
            .map(|(item_id, (score, reason))| {
                let tx = self.db.begin_transaction().unwrap();
                let item_props = tx.get_node_properties(item_id).unwrap();
                let title = item_props
                    .get("title")
                    .and_then(|v| v.as_string())
                    .unwrap_or("Unknown")
                    .to_string();

                Recommendation {
                    item_id,
                    title,
                    score,
                    reason,
                }
            })
            .collect();

        recommendations.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        recommendations.truncate(num_recommendations);
        Ok(recommendations)
    }

    /// Get popular items (fallback for cold start)
    fn get_popular_items(&self, num_items: usize) -> Result<Vec<Recommendation>> {
        let tx = self.db.begin_transaction()?;
        let items = tx.find_nodes_by_label("Item")?;

        let mut recommendations: Vec<Recommendation> = items
            .into_iter()
            .filter_map(|item| {
                let props = tx.get_node_properties(item.id).ok()?;
                let rating = props.get("rating").and_then(|v| v.as_float())?;
                let num_ratings = props.get("num_ratings").and_then(|v| v.as_integer())?;
                let title = props
                    .get("title")
                    .and_then(|v| v.as_string())
                    .map(|s| s.to_string())?;

                // Only recommend items with at least 5 ratings
                if num_ratings >= 5 {
                    Some(Recommendation {
                        item_id: item.id,
                        title,
                        score: rating,
                        reason: format!("Popular item ({} ratings)", num_ratings),
                    })
                } else {
                    None
                }
            })
            .collect();

        recommendations.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        recommendations.truncate(num_items);
        Ok(recommendations)
    }

    /// Get personalized recommendations for a user
    fn get_recommendations(
        &self,
        user_id: u64,
        num_recommendations: usize,
    ) -> Result<Vec<Recommendation>> {
        // Try hybrid recommendations first
        let hybrid_recs = self.hybrid_recommendations(user_id, num_recommendations, 0.6)?;

        if hybrid_recs.len() >= num_recommendations {
            return Ok(hybrid_recs);
        }

        // Fall back to collaborative filtering
        let collab_recs =
            self.collaborative_filtering_recommendations(user_id, num_recommendations)?;

        if collab_recs.len() >= num_recommendations {
            return Ok(collab_recs);
        }

        // Fall back to content-based
        let content_recs = self.content_based_recommendations(user_id, num_recommendations)?;

        if content_recs.len() >= num_recommendations {
            return Ok(content_recs);
        }

        // Final fallback: popular items
        self.get_popular_items(num_recommendations)
    }

    /// Get recommendation statistics
    fn get_recommendation_stats(&self) -> Result<RecommendationStats> {
        let tx = self.db.begin_transaction()?;

        let users = tx.find_nodes_by_label("User")?;
        let items = tx.find_nodes_by_label("Item")?;

        let mut total_ratings = 0;
        let mut rating_sum = 0.0;
        let mut user_rating_counts = HashMap::new();
        let mut item_rating_counts = HashMap::new();

        for user in &users {
            let user_ratings = self.get_user_ratings(user.id)?;
            user_rating_counts.insert(user.id, user_ratings.len());
            total_ratings += user_ratings.len();

            for (_, rating) in user_ratings {
                rating_sum += rating;
            }
        }

        for item in &items {
            let item_ratings = self.get_item_ratings(item.id)?;
            item_rating_counts.insert(item.id, item_ratings.len());
        }

        let avg_ratings_per_user = if users.is_empty() {
            0.0
        } else {
            total_ratings as f64 / users.len() as f64
        };

        let avg_ratings_per_item = if items.is_empty() {
            0.0
        } else {
            total_ratings as f64 / items.len() as f64
        };

        let avg_rating = if total_ratings == 0 {
            0.0
        } else {
            rating_sum / total_ratings as f64
        };

        Ok(RecommendationStats {
            total_users: users.len(),
            total_items: items.len(),
            total_ratings,
            avg_rating,
            avg_ratings_per_user,
            avg_ratings_per_item,
        })
    }
}

#[derive(Debug)]
struct RecommendationStats {
    total_users: usize,
    total_items: usize,
    total_ratings: usize,
    avg_rating: f64,
    avg_ratings_per_user: f64,
    avg_ratings_per_item: f64,
}

fn main() -> Result<()> {
    println!("🎯 Recommendation Engine Example with Sombra");

    // Initialize recommendation engine
    let mut engine = RecommendationEngine::new("recommendation_engine.db")?;

    // Add users
    let alice = engine.add_user(User {
        id: None,
        username: "alice_dev".to_string(),
        name: "Alice Developer".to_string(),
        age: 28,
        interests: vec![
            "technology".to_string(),
            "programming".to_string(),
            "ai".to_string(),
        ],
        location: Some("San Francisco".to_string()),
        join_date: chrono::Utc::now() - chrono::Duration::days(365),
    })?;

    let bob = engine.add_user(User {
        id: None,
        username: "bob_tech".to_string(),
        name: "Bob Technician".to_string(),
        age: 32,
        interests: vec![
            "technology".to_string(),
            "gadgets".to_string(),
            "gaming".to_string(),
        ],
        location: Some("Seattle".to_string()),
        join_date: chrono::Utc::now() - chrono::Duration::days(200),
    })?;

    let charlie = engine.add_user(User {
        id: None,
        username: "charlie_sci".to_string(),
        name: "Charlie Scientist".to_string(),
        age: 35,
        interests: vec![
            "science".to_string(),
            "research".to_string(),
            "ai".to_string(),
        ],
        location: Some("Boston".to_string()),
        join_date: chrono::Utc::now() - chrono::Duration::days(150),
    })?;

    let diana = engine.add_user(User {
        id: None,
        username: "diana_art".to_string(),
        name: "Diana Artist".to_string(),
        age: 26,
        interests: vec![
            "art".to_string(),
            "design".to_string(),
            "photography".to_string(),
        ],
        location: Some("New York".to_string()),
        join_date: chrono::Utc::now() - chrono::Duration::days(100),
    })?;

    println!("✅ Added 4 users");

    // Add items
    let rust_book = engine.add_item(Item {
        id: None,
        title: "The Rust Programming Language".to_string(),
        category: "Books".to_string(),
        tags: vec![
            "programming".to_string(),
            "rust".to_string(),
            "technology".to_string(),
        ],
        description: Some("Comprehensive guide to Rust programming".to_string()),
        rating: 4.5,
        num_ratings: 150,
        created_date: chrono::Utc::now() - chrono::Duration::days(30),
    })?;

    let ai_course = engine.add_item(Item {
        id: None,
        title: "Introduction to Machine Learning".to_string(),
        category: "Courses".to_string(),
        tags: vec![
            "ai".to_string(),
            "machine learning".to_string(),
            "technology".to_string(),
        ],
        description: Some("Learn the fundamentals of ML".to_string()),
        rating: 4.8,
        num_ratings: 200,
        created_date: chrono::Utc::now() - chrono::Duration::days(20),
    })?;

    let gaming_mouse = engine.add_item(Item {
        id: None,
        title: "Pro Gaming Mouse RGB".to_string(),
        category: "Electronics".to_string(),
        tags: vec![
            "gaming".to_string(),
            "gadgets".to_string(),
            "electronics".to_string(),
        ],
        description: Some("High-precision gaming mouse".to_string()),
        rating: 4.2,
        num_ratings: 80,
        created_date: chrono::Utc::now() - chrono::Duration::days(15),
    })?;

    let camera = engine.add_item(Item {
        id: None,
        title: "Digital Photography Camera".to_string(),
        category: "Electronics".to_string(),
        tags: vec![
            "photography".to_string(),
            "art".to_string(),
            "electronics".to_string(),
        ],
        description: Some("Professional digital camera".to_string()),
        rating: 4.6,
        num_ratings: 120,
        created_date: chrono::Utc::now() - chrono::Duration::days(10),
    })?;

    let science_podcast = engine.add_item(Item {
        id: None,
        title: "Science Today Podcast".to_string(),
        category: "Podcasts".to_string(),
        tags: vec![
            "science".to_string(),
            "research".to_string(),
            "education".to_string(),
        ],
        description: Some("Latest science news and discoveries".to_string()),
        rating: 4.3,
        num_ratings: 90,
        created_date: chrono::Utc::now() - chrono::Duration::days(5),
    })?;

    let design_software = engine.add_item(Item {
        id: None,
        title: "Creative Design Suite Pro".to_string(),
        category: "Software".to_string(),
        tags: vec![
            "design".to_string(),
            "art".to_string(),
            "creative".to_string(),
        ],
        description: Some("Professional design software".to_string()),
        rating: 4.4,
        num_ratings: 110,
        created_date: chrono::Utc::now() - chrono::Duration::days(3),
    })?;

    println!("✅ Added 6 items");

    // Add ratings
    let now = chrono::Utc::now();

    // Alice's ratings (technology/programming focused)
    engine.add_rating(Rating {
        id: None,
        user_id: alice.id,
        item_id: rust_book.id,
        rating: 5.0,
        timestamp: now - chrono::Duration::hours(24),
    })?;

    engine.add_rating(Rating {
        id: None,
        user_id: alice.id,
        item_id: ai_course.id,
        rating: 4.0,
        timestamp: now - chrono::Duration::hours(12),
    })?;

    engine.add_rating(Rating {
        id: None,
        user_id: alice.id,
        item_id: gaming_mouse.id,
        rating: 3.0,
        timestamp: now - chrono::Duration::hours(6),
    })?;

    // Bob's ratings (technology/gaming focused)
    engine.add_rating(Rating {
        id: None,
        user_id: bob.id,
        item_id: rust_book.id,
        rating: 4.0,
        timestamp: now - chrono::Duration::hours(20),
    })?;

    engine.add_rating(Rating {
        id: None,
        user_id: bob.id,
        item_id: gaming_mouse.id,
        rating: 5.0,
        timestamp: now - chrono::Duration::hours(15),
    })?;

    engine.add_rating(Rating {
        id: None,
        user_id: bob.id,
        item_id: ai_course.id,
        rating: 3.5,
        timestamp: now - chrono::Duration::hours(8),
    })?;

    // Charlie's ratings (science/ai focused)
    engine.add_rating(Rating {
        id: None,
        user_id: charlie.id,
        item_id: ai_course.id,
        rating: 5.0,
        timestamp: now - chrono::Duration::hours(18),
    })?;

    engine.add_rating(Rating {
        id: None,
        user_id: charlie.id,
        item_id: science_podcast.id,
        rating: 4.5,
        timestamp: now - chrono::Duration::hours(10),
    })?;

    engine.add_rating(Rating {
        id: None,
        user_id: charlie.id,
        item_id: rust_book.id,
        rating: 3.0,
        timestamp: now - chrono::Duration::hours(4),
    })?;

    // Diana's ratings (art/design focused)
    engine.add_rating(Rating {
        id: None,
        user_id: diana.id,
        item_id: camera.id,
        rating: 5.0,
        timestamp: now - chrono::Duration::hours(16),
    })?;

    engine.add_rating(Rating {
        id: None,
        user_id: diana.id,
        item_id: design_software.id,
        rating: 4.5,
        timestamp: now - chrono::Duration::hours(9),
    })?;

    engine.add_rating(Rating {
        id: None,
        user_id: diana.id,
        item_id: gaming_mouse.id,
        rating: 2.0,
        timestamp: now - chrono::Duration::hours(3),
    })?;

    println!("✅ Added ratings");

    // Demonstrate recommendation features
    println!("\n🎯 Recommendation Analysis:");

    // Get recommendations for each user
    for (user_id, name) in [
        (alice.id, "Alice"),
        (bob.id, "Bob"),
        (charlie.id, "Charlie"),
        (diana.id, "Diana"),
    ] {
        println!("\n📋 Recommendations for {}:", name);

        let recommendations = engine.get_recommendations(user_id, 5)?;
        if recommendations.is_empty() {
            println!("  No recommendations available");
        } else {
            for (i, rec) in recommendations.iter().enumerate() {
                println!("  {}. {} (Score: {:.2})", i + 1, rec.title, rec.score);
                println!("     Reason: {}", rec.reason);
            }
        }

        // Show different recommendation types
        println!("\n  🔍 Collaborative Filtering:");
        let collab_recs = engine.collaborative_filtering_recommendations(user_id, 3)?;
        for rec in collab_recs {
            println!("    - {} ({:.2})", rec.title, rec.score);
        }

        println!("\n  📚 Content-Based:");
        let content_recs = engine.content_based_recommendations(user_id, 3)?;
        for rec in content_recs {
            println!("    - {} ({:.2})", rec.title, rec.score);
        }
    }

    // Find similar users
    println!("\n👥 Similar Users Analysis:");
    let alice_similar = engine.find_similar_users(alice.id, 2)?;
    println!("Users similar to Alice:");
    for (user_id, similarity) in alice_similar.iter().take(3) {
        let tx = engine.db.begin_transaction()?;
        let user_node = tx.get_node(*user_id)?;
        let user_props = tx.get_node_properties(*user_id)?;
        let name = user_props
            .get("name")
            .and_then(|v| v.as_string())
            .unwrap_or("Unknown");
        println!("  - {} (Similarity: {:.3})", name, similarity);
    }

    // Popular items (fallback recommendations)
    println!("\n🔥 Popular Items:");
    let popular = engine.get_popular_items(5)?;
    for (i, item) in popular.iter().enumerate() {
        println!("  {}. {} (Rating: {:.1})", i + 1, item.title, item.score);
    }

    // Statistics
    let stats = engine.get_recommendation_stats()?;
    println!("\n📊 Recommendation Engine Statistics:");
    println!("Total users: {}", stats.total_users);
    println!("Total items: {}", stats.total_items);
    println!("Total ratings: {}", stats.total_ratings);
    println!("Average rating: {:.2}", stats.avg_rating);
    println!(
        "Average ratings per user: {:.1}",
        stats.avg_ratings_per_user
    );
    println!(
        "Average ratings per item: {:.1}",
        stats.avg_ratings_per_item
    );

    println!("\n🎉 Recommendation engine example completed successfully!");
    println!("Database saved to: recommendation_engine.db");

    Ok(())
}
