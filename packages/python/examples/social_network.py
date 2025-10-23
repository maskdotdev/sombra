#!/usr/bin/env python3
"""
Social Network Example

This example demonstrates building a social network graph with Sombra,
including user profiles, friendships, posts, and comments.
"""

import sombra
import time


class SocialNetwork:
    def __init__(self, db_path):
        self.db = sombra.SombraDB(db_path)

    def create_user(self, username, name, email, age, bio=None, location=None):
        tx = self.db.begin_transaction()

        properties = {
            "username": username,
            "name": name,
            "email": email,
            "age": age,
            "joined_at": int(time.time()),
            "last_active": int(time.time()),
        }

        if bio:
            properties["bio"] = bio
        if location:
            properties["location"] = location

        user_id = tx.add_node(["User"], properties)
        tx.commit()
        return user_id

    def create_friendship(self, user1_id, user2_id, since):
        tx = self.db.begin_transaction()
        edge_id = tx.add_edge(user1_id, user2_id, "FRIENDS_WITH", {"since": since})
        tx.commit()
        return edge_id

    def get_user_friends(self, user_id):
        tx = self.db.begin_transaction()
        neighbors = tx.get_neighbors(user_id)
        tx.commit()
        return neighbors

    def create_post(self, author_id, content, likes=0, shares=0):
        tx = self.db.begin_transaction()

        post_id = tx.add_node(
            ["Post"],
            {
                "content": content,
                "created_at": int(time.time()),
                "likes": likes,
                "shares": shares,
            },
        )

        tx.add_edge(author_id, post_id, "POSTED", {})
        tx.commit()
        return post_id

    def create_comment(self, post_id, author_id, content):
        tx = self.db.begin_transaction()

        comment_id = tx.add_node(
            ["Comment"], {"content": content, "created_at": int(time.time())}
        )

        tx.add_edge(author_id, comment_id, "COMMENTED", {})
        tx.add_edge(comment_id, post_id, "ON_POST", {})
        tx.commit()
        return comment_id

    def get_user_posts(self, user_id):
        tx = self.db.begin_transaction()
        outgoing = tx.get_outgoing_edges(user_id)

        post_ids = []
        for edge_id in outgoing:
            edge = tx.get_edge(edge_id)
            if edge.type_name == "POSTED":
                post_ids.append(edge.target_node_id)

        tx.commit()
        return post_ids

    def get_post_comments(self, post_id):
        tx = self.db.begin_transaction()
        incoming = tx.get_incoming_edges(post_id)

        comment_ids = []
        for edge_id in incoming:
            edge = tx.get_edge(edge_id)
            if edge.type_name == "ON_POST":
                comment_ids.append(edge.source_node_id)

        tx.commit()
        return comment_ids


def main():
    print("🌐 Social Network Example with Sombra")

    network = SocialNetwork("social_network.db")

    now = int(time.time())

    alice_id = network.create_user(
        username="alice_smith",
        name="Alice Smith",
        email="alice@example.com",
        age=28,
        bio="Software engineer who loves hiking and photography",
        location="San Francisco, CA",
    )

    bob_id = network.create_user(
        username="bob_jones",
        name="Bob Jones",
        email="bob@example.com",
        age=32,
        bio="Data scientist and coffee enthusiast",
        location="Seattle, WA",
    )

    charlie_id = network.create_user(
        username="charlie_brown",
        name="Charlie Brown",
        email="charlie@example.com",
        age=29,
        bio="Product manager and tech blogger",
        location="Austin, TX",
    )

    print("✅ Created 3 users")

    network.create_friendship(alice_id, bob_id, now - (180 * 86400))
    network.create_friendship(bob_id, charlie_id, now - (90 * 86400))
    network.create_friendship(alice_id, charlie_id, now - (60 * 86400))

    print("✅ Created friendships")

    post1_id = network.create_post(
        alice_id,
        "Just finished an amazing hike in the mountains! The view was breathtaking.",
        likes=5,
        shares=2,
    )

    post2_id = network.create_post(
        bob_id,
        "Excited to share that I'll be speaking at the Data Science Conference next month!",
        likes=12,
        shares=8,
    )

    post3_id = network.create_post(
        charlie_id,
        "New blog post: Top 10 Product Management Tips for 2024",
        likes=8,
        shares=5,
    )

    print("✅ Created posts")

    network.create_comment(
        post1_id, bob_id, "Looks incredible! Which trail did you take?"
    )

    network.create_comment(
        post1_id, charlie_id, "Beautiful! I need to get out hiking more often."
    )

    network.create_comment(
        post2_id, alice_id, "Congratulations! I'll be there to support you."
    )

    print("✅ Created comments")

    print("\n📊 Social Network Analysis:")

    alice_friends = network.get_user_friends(alice_id)
    print(f"Alice's friends: {len(alice_friends)}")

    alice_posts = network.get_user_posts(alice_id)
    print(f"Alice's posts: {len(alice_posts)}")

    bob_posts = network.get_user_posts(bob_id)
    if bob_posts:
        post2_comments = network.get_post_comments(bob_posts[0])
        print(f"Comments on Bob's conference post: {len(post2_comments)}")

    post1_comments = network.get_post_comments(post1_id)
    print(f"Comments on Alice's hiking post: {len(post1_comments)}")

    print("\n🎉 Social network example completed successfully!")
    print("Database saved to: social_network.db")


if __name__ == "__main__":
    main()
