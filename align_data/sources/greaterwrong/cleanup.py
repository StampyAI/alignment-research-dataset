"""
Utilities for cleaning up non-AI posts from LessWrong in the database.
"""

import logging
import json
from sqlalchemy import and_

from align_data.db.session import make_session
from align_data.db.models import Article, PineconeStatus

logger = logging.getLogger(__name__)


def check_post_has_ai_tag(post):
    """Check if the post has the AI tag."""
    # Check if metadata contains tags
    if not post.meta:
        return False
        
    # Parse metadata to get tags
    meta = json.loads(post.meta) if isinstance(post.meta, str) else post.meta
    tags = meta.get('tags', [])
    
    # Check for 'AI' tag
    return 'AI' in tags


def cleanup_lesswrong_posts(dry_run=False, source='lesswrong'):
    """Mark non-AI LessWrong posts as invalid."""
    with make_session() as session:
        # Get all LessWrong posts that are currently marked as valid
        posts = (
            session.query(Article)
            .filter(Article.source == source)
            .filter(Article.is_valid)
            .all()
        )
        
        logger.info(f"Found {len(posts)} valid {source} posts")
        
        posts_to_mark_invalid = []
        
        for post in posts:
            has_ai_tag = check_post_has_ai_tag(post)
            
            if not has_ai_tag:
                posts_to_mark_invalid.append(post)
                logger.info(f"Post without AI tag: {post.title} ({post.url})")
        
        logger.info(f"Found {len(posts_to_mark_invalid)} {source} posts without AI tag")
        
        if not dry_run and posts_to_mark_invalid:
            for post in posts_to_mark_invalid:
                # Mark as invalid and set status to explain why
                post.status = "Missing AI tag"
                post.pinecone_status = PineconeStatus.pending_removal
                post.append_comment("Automatically marked as invalid because it lacks the AI tag")
            
            session.commit()
            logger.info(f"Updated {len(posts_to_mark_invalid)} posts to invalid status")
        elif posts_to_mark_invalid:
            logger.info("Dry run - no changes made to database")