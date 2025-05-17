#!/usr/bin/env python
"""
Utilities for cleaning up posts that don't match required tags in the database.
Can be run as a standalone command-line script for maintenance.
"""

import logging
import json
import argparse
import sys
from typing import Dict, List, Set, Optional

from align_data.db.session import make_session
from align_data.db.models import Article, PineconeStatus
from align_data.sources.greaterwrong.config import SOURCE_CONFIG

logger = logging.getLogger(__name__)


def check_post_meets_tag_requirements(post, tag_requirements):
    if tag_requirements.get("bypass_tag_check", False):
        return True
        
    # Check if metadata contains tags
    if not post.meta:
        return False

    meta = json.loads(post.meta) if isinstance(post.meta, str) else post.meta
    post_tags = set(meta.get("tags", []))
    
    # If post has no tags and we have required tags, it fails
    if not post_tags and tag_requirements.get("required_tags"):
        return False
    
    # Check for required tags - at least one must be present
    required_tags = set(tag_requirements.get("required_tags", []))
    if required_tags and not required_tags.intersection(post_tags):
        return False
                
    # Check for excluded tags - none must be present
    excluded_tags = set(tag_requirements.get("excluded_tags", []))
    if excluded_tags and excluded_tags.intersection(post_tags):
        return False
        
    return True


def cleanup_posts_by_tags(
    source: str, 
    dry_run: bool = False
):
    """
    Mark posts that don't meet tag requirements as invalid.
    
    Args:
        source: The name of the source to clean up
        dry_run: If True, don't actually modify the database
    """
    tag_requirements = SOURCE_CONFIG.get(source)
    
    if not tag_requirements:
        logger.warning(f"No tag requirements defined for source '{source}', skipping cleanup")
        return
        
    with make_session() as session:
        # Get all posts from the specified source that are currently marked as valid
        posts = (
            session.query(Article).filter(Article.source == source).filter(Article.is_valid).all()
        )

        logger.info(f"Found {len(posts)} valid {source} posts")

        posts_to_mark_invalid = []

        for post in posts:
            meets_requirements = check_post_meets_tag_requirements(post, tag_requirements)

            if not meets_requirements:
                posts_to_mark_invalid.append(post)
                logger.info(f"Post doesn't meet tag requirements: {post.title} ({post.url})")

        logger.info(f"Found {len(posts_to_mark_invalid)} {source} posts that don't meet tag requirements")

        if not dry_run and posts_to_mark_invalid:
            for post in posts_to_mark_invalid:
                # Mark as invalid and set status to explain why
                post.status = "Does not meet tag requirements"
                post.pinecone_status = PineconeStatus.pending_removal
                post.append_comment("Automatically marked as invalid because it doesn't meet tag requirements")

            session.commit()
            logger.info(f"Updated {len(posts_to_mark_invalid)} posts to invalid status")
        elif posts_to_mark_invalid:
            logger.info("Dry run - no changes made to database")


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Clean up posts based on tag requirements")
    
    parser.add_argument(
        "source",
        help="The data source to clean up (e.g., 'lesswrong')",
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    
    return parser.parse_args()



if __name__ == "__main__":
    # Configure basic logging for command-line usage
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    
    args = parse_args()
    source = args.source
    
    # Check if source exists in configuration
    if source not in SOURCE_CONFIG:
        logger.error(f"No configuration defined for source '{source}'. Available sources: {list(SOURCE_CONFIG.keys())}")
        sys.exit(1)
    
    # Run the cleanup
    logger.info(f"Cleaning up {source} posts")
    if args.dry_run:
        logger.info("Dry run mode - no changes will be made")
    
    cleanup_posts_by_tags(source, args.dry_run)
    
    logger.info("Cleanup completed")
