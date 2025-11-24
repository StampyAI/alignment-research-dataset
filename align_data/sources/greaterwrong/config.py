"""
Shared configuration for the GreaterWrong data sources.
This serves as the single source of truth for filtering rules and query parameters.
"""

# Configuration for each source
# Format: {
#   source_name: {
#     # Tag filtering settings
#     required_tags: [],          # At least one must be present
#     excluded_tags: [],          # None can be present
#     bypass_tag_check: bool,     # Whether to skip tag checking
#
#     # GraphQL query parameters
#     exclude_events: bool,       # Whether to exclude events in the query
#     karma_threshold: int,       # Minimum karma required
#   }
# }
SOURCE_CONFIG = {
    "lesswrong": {
        "required_tags": ["AI"],
        "excluded_tags": [],
        "bypass_tag_check": False,
        "exclude_events": True, 
        "karma_threshold": 1,
    },
    "eaforum": {
        "required_tags": ["AI safety"],
        "excluded_tags": [],
        "bypass_tag_check": False,
        "exclude_events": True,
        "karma_threshold": 1,
    },
    "alignmentforum": {
        "required_tags": [],
        "excluded_tags": [],
        "bypass_tag_check": True,
        "exclude_events": True,
        "karma_threshold": 1,
    }
}

def get_source_config(source):
    """Get the configuration for a specific source."""
    return SOURCE_CONFIG.get(source)

# For backward compatibility
TAG_REQUIREMENTS = SOURCE_CONFIG
def get_tag_requirements(source):
    return get_source_config(source)