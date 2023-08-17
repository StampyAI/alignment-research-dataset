from align_data.sources.blogs.wp_blog import WordpressBlog
from align_data.sources.blogs.gwern_blog import GwernBlog
from align_data.sources.blogs.blogs import (
    ColdTakes,
    GenerativeInk,
    CaradoMoe,
    EleutherAI,
    OpenAIResearch,
    DeepMindTechnicalBlog,
)
from align_data.sources.blogs.substack_blog import SubstackBlog
from align_data.sources.articles.parsers import MediumParser


BLOG_REGISTRY = [
    WordpressBlog(name="aiimpacts", url="https://aiimpacts.org"),
    WordpressBlog(name="aisafety.camp", url="https://aisafety.camp"),
    WordpressBlog(name="miri", url="https://intelligence.org"),
    WordpressBlog(name="jsteinhardt_blog", url="https://jsteinhardt.wordpress.com"),
    WordpressBlog(name="vkrakovna_blog", url="https://vkrakovna.wordpress.com"),
    WordpressBlog(name="yudkowsky_blog", url="https://yudkowsky.net"),
    MediumParser(
        name="deepmind_blog",
        url="https://deepmindsafetyresearch.medium.com/",
        authors=["DeepMind Safety Research"],
    ),
    GwernBlog(
        name="gwern_blog", url="https://www.gwern.net/", authors=["Gwern Branwen"]
    ),
    ColdTakes(
        name="cold_takes",
        url="https://www.cold-takes.com/",
        authors=["Holden Karnofsky"],
    ),
    GenerativeInk(
        name="generative.ink",
        url="https://generative.ink/posts/",
        authors=["janus"],
    ),
    CaradoMoe(
        name="carado.moe",
        url="https://carado.moe",
        authors=["Tamsin Leake"],
    ),
    SubstackBlog(
        name="importai",
        url="https://importai.substack.com",
        id_fields=["url", "title", "source"],
    ),
    SubstackBlog(
        name="ml_safety_newsletter",
        url="https://newsletter.mlsafety.org",
        id_fields=["url", "title", "source"],
    ),
    EleutherAI(name="eleuther.ai", url="https://blog.eleuther.ai/"),
    OpenAIResearch(name="openai.research", url="https://openai.com/research"),
    DeepMindTechnicalBlog(
        name="deepmind_technical_blog",
        url="https://www.deepmind.com/blog-categories/technical-blogs",
    ),
]
