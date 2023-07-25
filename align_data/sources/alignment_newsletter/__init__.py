from .alignment_newsletter import AlignmentNewsletter
import os

ALIGNMENT_NEWSLETTER_REGISTRY = [
        AlignmentNewsletter( 
                name = "alignment_newsletter" , id_fields=['url', 'title', 'source']
        ),
]